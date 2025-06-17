import json
import logging
import pandas as pd
import joblib
import numpy as np  # <-- Import numpy
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from datetime import datetime, UTC
import time

# --- 1. Centralized Configuration ---
CONFIG = {
    "kafka_bootstrap_servers": ['localhost:9092'],
    "kafka_topic": 'processed_network_flows',
    "kafka_consumer_group": 'ddos_detector_group',
    "es_host": "http://localhost:9200",
    "es_index": "ddos_predictions",
    "artifact_path": 'LR_model_artifacts.pkl',
    # --- New Class Map for your 3-class model ---
    "class_map": {
        0: "Attack",
        1: "Benign",
        2: "Suspicious"
    }
}

# --- 2. Setup Proper Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_artifact(path):
    """Loads the machine learning artifact and handles errors."""
    try:
        artifact = joblib.load(path)
        logging.info(f"Successfully loaded artifact from {path}")
        # Log the class order the model learned during training
        logging.info(f"Model class order detected: {artifact['model'].classes_}")
        return artifact
    except FileNotFoundError:
        logging.error(f"Artifact file not found at {path}. Please check the path.")
        return None
    except Exception as e:
        logging.error(f"An error occurred while loading the artifact: {e}")
        return None

def process_flow(flow_data, ml_artifact, es_client):
    """
    Processes a single flow: preprocesses, predicts for multi-class, and stores the result.
    """
    try:
        model = ml_artifact['model']
        scaler = ml_artifact['scaler']
        feature_order = ml_artifact['feature_order']
        model_classes = model.classes_ # The learned order, e.g., [0, 1, 2]

        # --- Preprocess the received flow ---
        live_flow_df = pd.DataFrame([flow_data])
        for col in feature_order:
            if col not in live_flow_df.columns:
                live_flow_df[col] = 0
        live_flow_df = live_flow_df[feature_order]
        scaled_features = scaler.transform(live_flow_df)

        # --- Multi-Class Prediction ---
        # Get probabilities for all classes, e.g., [[P(class_0), P(class_1), P(class_2)]]
        probabilities = model.predict_proba(scaled_features)[0]
        
        # Find the index of the class with the highest probability
        predicted_index = np.argmax(probabilities)
        
        # Get the label (e.g., "Attack") and confidence score for the winning class
        prediction_label = CONFIG['class_map'].get(predicted_index, "Unknown")
        confidence_score = probabilities[predicted_index]
        
        # Create a dictionary of all probabilities for storage
        all_probabilities = {CONFIG['class_map'].get(cls, f"Unknown_{cls}"): prob for cls, prob in zip(model_classes, probabilities)}

        # --- Store Results in Elasticsearch ---
        document = {
            "@timestamp": datetime.now(UTC),
            "prediction": prediction_label,
            "confidence": confidence_score,
            "all_probabilities": all_probabilities, # Store all class probabilities
            "flow_data": flow_data
        }
        
        try:
            es_client.index(index=CONFIG['es_index'], document=document)
            logging.info(f"Processed flow. Prediction: {prediction_label} (Confidence: {confidence_score:.4f})")
        except es_exceptions.ConnectionError as e:
            logging.error(f"Could not connect to Elasticsearch to store result. Error: {e}")

    except KeyError as e:
        logging.warning(f"A key was missing from the flow data: {e}. Skipping this message.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during flow processing: {e}")

def main():
    """Main function to run the Kafka consumer."""
    logging.info("Starting multi-class DDoS detection consumer...")

    ml_artifact = load_artifact(CONFIG['artifact_path'])
    if not ml_artifact:
        return

    es_client = Elasticsearch(CONFIG['es_host'])
    
    while True:
        try:
            consumer = KafkaConsumer(
                CONFIG['kafka_topic'],
                bootstrap_servers=CONFIG['kafka_bootstrap_servers'],
                auto_offset_reset='latest',
                group_id=CONFIG['kafka_consumer_group'],
                value_deserializer=lambda v: json.loads(v.decode('utf-8', 'ignore'))
            )
            logging.info("Successfully connected to Kafka.")
            break
        except Exception as e:
            logging.error(f"Could not connect to Kafka, retrying in 5 seconds... Error: {e}")
            time.sleep(5)

    for message in consumer:
        process_flow(message.value, ml_artifact, es_client)

if __name__ == "__main__":
    main()