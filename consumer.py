import json
import logging
import pandas as pd
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
    "es_index": "ddos_predictions"
}

# --- 2. Setup Proper Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_message(message_value, es_client):
    """
    Processes a single Kafka message: expects 'flow', 'prediction', 'confidence', 'all_probabilities'.
    Stores the result in Elasticsearch.
    """
    try:
        # Validate message structure
        required_keys = ["flow", "prediction", "confidence", "all_probabilities"]
        if not all(key in message_value for key in required_keys):
            logging.warning(f"Kafka message missing required keys: {message_value}")
            return

        document = {
            "@timestamp": datetime.now(UTC),
            "prediction": message_value["prediction"],
            "confidence": message_value["confidence"],
            "all_probabilities": message_value["all_probabilities"],
            "flow_data": message_value["flow"]
        }
        try:
            es_client.index(index=CONFIG['es_index'], document=document)
            logging.info(f"Stored flow with prediction: {message_value['prediction']} (Confidence: {message_value['confidence']:.4f})")
        except es_exceptions.ConnectionError as e:
            logging.error(f"Could not connect to Elasticsearch to store result. Error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during message processing: {e}")

def main():
    """Main function to run the Kafka consumer."""
    logging.info("Starting DDoS detection consumer (no model inference, just storing results)...")

    # Initialize Elasticsearch client with compatible version
    es_client = Elasticsearch(
        CONFIG['es_host'],
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )
    
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
        process_message(message.value, es_client)

if __name__ == "__main__":
    main()