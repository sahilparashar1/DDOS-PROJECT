import json
import logging
import pandas as pd
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from datetime import datetime, UTC
import time
from fluentd_logger import fluent_logger

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
            fluent_logger.log_error("consumer", "Invalid message structure", {
                "missing_keys": [key for key in required_keys if key not in message_value],
                "message_keys": list(message_value.keys())
            })
            return

        # Extract flow metadata for logging
        flow_data = message_value["flow"]
        flow_metadata = {
            "src_ip": flow_data.get("src_ip", "unknown"),
            "dst_ip": flow_data.get("dst_ip", "unknown"),
            "src_port": flow_data.get("src_port", "unknown"),
            "dst_port": flow_data.get("dst_port", "unknown"),
            "protocol": flow_data.get("protocol", "unknown")
        }

        document = {
            "@timestamp": datetime.now(UTC),
            "prediction": message_value["prediction"],
            "confidence": message_value["confidence"],
            "all_probabilities": message_value["all_probabilities"],
            "flow_data": message_value["flow"],
            "batch_id": message_value.get("batch_id", "unknown"),
            "flow_id": message_value.get("flow_id", "unknown")
        }
        
        try:
            es_start_time = time.time()
            es_client.index(index=CONFIG['es_index'], document=document)
            es_time = (time.time() - es_start_time) * 1000  # Convert to milliseconds
            
            logging.info(f"Stored flow with prediction: {message_value['prediction']} (Confidence: {message_value['confidence']:.4f})")
            
            # Log successful storage
            fluent_logger.log_consumer_event("prediction_stored", {
                "prediction": message_value['prediction'],
                "confidence": message_value['confidence'],
                "flow_metadata": flow_metadata,
                "batch_id": message_value.get("batch_id", "unknown"),
                "flow_id": message_value.get("flow_id", "unknown"),
                "es_index": CONFIG['es_index'],
                "storage_time_ms": es_time
            })
            
            # Log performance metric
            fluent_logger.log_performance("consumer", "elasticsearch_storage_time", es_time, "ms")
            
        except es_exceptions.ConnectionError as e:
            logging.error(f"Could not connect to Elasticsearch to store result. Error: {e}")
            fluent_logger.log_error("consumer", f"Elasticsearch connection error: {e}", {
                "es_host": CONFIG['es_host'],
                "es_index": CONFIG['es_index'],
                "flow_metadata": flow_metadata
            })
        except Exception as e:
            logging.error(f"Elasticsearch storage error: {e}")
            fluent_logger.log_error("consumer", f"Elasticsearch storage failed: {e}", {
                "es_index": CONFIG['es_index'],
                "flow_metadata": flow_metadata,
                "prediction": message_value['prediction']
            })
            
    except Exception as e:
        logging.error(f"An unexpected error occurred during message processing: {e}")
        fluent_logger.log_error("consumer", f"Message processing failed: {e}", {
            "message_keys": list(message_value.keys()) if isinstance(message_value, dict) else "not_dict"
        })

def main():
    """Main function to run the Kafka consumer."""
    logging.info("Starting DDoS detection consumer (no model inference, just storing results)...")
    
    # Log consumer startup
    fluent_logger.log_consumer_event("consumer_started", {
        "kafka_servers": CONFIG['kafka_bootstrap_servers'],
        "kafka_topic": CONFIG['kafka_topic'],
        "consumer_group": CONFIG['kafka_consumer_group'],
        "es_host": CONFIG['es_host'],
        "es_index": CONFIG['es_index']
    })

    # Initialize Elasticsearch client with compatible version
    es_client = Elasticsearch(
        CONFIG['es_host'],
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )
    
    # Test Elasticsearch connection
    try:
        es_client.ping()
        fluent_logger.log_consumer_event("elasticsearch_connected", {
            "status": "success",
            "es_host": CONFIG['es_host']
        })
    except Exception as e:
        fluent_logger.log_error("consumer", f"Elasticsearch connection test failed: {e}", {
            "es_host": CONFIG['es_host']
        })
    
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
            fluent_logger.log_consumer_event("kafka_connected", {
                "status": "success",
                "kafka_servers": CONFIG['kafka_bootstrap_servers'],
                "topic": CONFIG['kafka_topic']
            })
            break
        except Exception as e:
            logging.error(f"Could not connect to Kafka, retrying in 5 seconds... Error: {e}")
            fluent_logger.log_error("consumer", f"Kafka connection failed: {e}", {
                "kafka_servers": CONFIG['kafka_bootstrap_servers'],
                "retry_delay": 5
            })
            time.sleep(5)

    # Start consuming messages
    message_count = 0
    start_time = time.time()
    
    fluent_logger.log_consumer_event("consumer_listening", {
        "topic": CONFIG['kafka_topic'],
        "consumer_group": CONFIG['kafka_consumer_group']
    })

    for message in consumer:
        message_count += 1
        message_start_time = time.time()
        
        try:
            process_message(message.value, es_client)
            message_time = (time.time() - message_start_time) * 1000  # Convert to milliseconds
            
            # Log message processing performance
            fluent_logger.log_performance("consumer", "message_processing_time", message_time, "ms")
            
            # Log periodic statistics
            if message_count % 100 == 0:
                elapsed_time = time.time() - start_time
                messages_per_second = message_count / elapsed_time if elapsed_time > 0 else 0
                
                fluent_logger.log_consumer_event("consumer_statistics", {
                    "message_count": message_count,
                    "elapsed_time_seconds": elapsed_time,
                    "messages_per_second": messages_per_second
                })
                
                logging.info(f"Processed {message_count} messages in {elapsed_time:.2f} seconds ({messages_per_second:.2f} msg/s)")
                
        except Exception as e:
            logging.error(f"Error processing message {message_count}: {e}")
            fluent_logger.log_error("consumer", f"Message processing error: {e}", {
                "message_number": message_count,
                "message_offset": message.offset,
                "message_partition": message.partition
            })

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user. Shutting down gracefully.")
        fluent_logger.log_consumer_event("consumer_shutdown", {"reason": "user_interrupt"})
        fluent_logger.close()
    except Exception as e:
        logging.error(f"Fatal error in consumer: {e}")
        fluent_logger.log_error("consumer", f"Fatal error: {e}")
        fluent_logger.close()
        raise