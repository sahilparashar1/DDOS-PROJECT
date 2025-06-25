import subprocess
import time
import pandas as pd
import json
from kafka import KafkaProducer
import os
import datetime
import sys
import requests
import atexit
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
TIME_WINDOW_SECONDS = 20
INTERFACE_NAME = '8'  # Change as needed
KAFKA_TOPIC = 'processed_network_flows'
NTL_EXECUTABLE_PATH = 'NTLFlowLyzer'  # Or the full path if not in your system's PATH
CONFIG_TEMPLATE_PATH = 'configg_template.json'
API_BASE_URL = "http://localhost:8000"
MAX_WORKERS = 4  # Number of threads for parallel API calls

# Global variables
temp_files_to_cleanup = []
processing_queue = queue.Queue()
stop_processing = threading.Event()

def get_timestamp_str():
    return datetime.datetime.now().strftime("%d_%m_%y_%H_%M_%S")

def cleanup_temp_files():
    """Clean up all temporary config files"""
    global temp_files_to_cleanup
    for temp_file in temp_files_to_cleanup:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"Cleaned up temp file: {temp_file}")
        except Exception as e:
            print(f"Warning: Could not delete temp file {temp_file}: {e}")
    temp_files_to_cleanup.clear()

def cleanup_single_temp_file(temp_file):
    """Clean up a single temporary config file"""
    try:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"Cleaned up temp file: {temp_file}")
            if temp_file in temp_files_to_cleanup:
                temp_files_to_cleanup.remove(temp_file)
    except Exception as e:
        print(f"Warning: Could not delete temp file {temp_file}: {e}")

def process_flow_with_api(flow_dict, producer, flow_id):
    """Process a single flow with the FastAPI and send to Kafka"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/predict",
            json={"flow": flow_dict},
            timeout=10
        )
        if response.status_code == 200:
            result = response.json()
            # Send to Kafka
            if producer:
                kafka_message = {
                    "flow": flow_dict,
                    "prediction": result["prediction"],
                    "confidence": result["confidence"],
                    "all_probabilities": result["all_probabilities"],
                    "flow_id": flow_id,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                producer.send(KAFKA_TOPIC, kafka_message)
            return True, result["prediction"]
        else:
            print(f"FastAPI error for flow {flow_id}: {response.status_code} {response.text}")
            return False, None
    except Exception as e:
        print(f"Error processing flow {flow_id}: {e}")
        return False, None

def process_csv_file(csv_file, producer):
    """Process CSV file and send flows to API in parallel"""
    if not os.path.exists(csv_file):
        print(f"Warning: CSV file {csv_file} does not exist.")
        return 0
    
    try:
        print(f"Reading CSV file: {csv_file}")
        df_flows = pd.read_csv(csv_file)
        
        # Clean the raw data before sending to FastAPI
        df_flows = df_flows.replace([float('inf'), float('-inf')], 0)
        df_flows = df_flows.fillna(0)
        df_flows = df_flows.replace({'handshake_duration': 'not a complete handshake'}, -1)
        df_flows['handshake_duration'] = pd.to_numeric(df_flows['handshake_duration'], errors='coerce').fillna(-1)
        
        print(f"Processing {len(df_flows)} flows in parallel...")
        
        # Process flows in parallel using ThreadPoolExecutor
        successful_predictions = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all flows for processing
            future_to_flow = {
                executor.submit(process_flow_with_api, row.to_dict(), producer, idx): idx 
                for idx, (_, row) in enumerate(df_flows.iterrows())
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_flow):
                flow_id = future_to_flow[future]
                try:
                    success, prediction = future.result()
                    if success:
                        successful_predictions += 1
                        if successful_predictions % 10 == 0:  # Log every 10 successful predictions
                            print(f"Processed {successful_predictions} flows...")
                except Exception as e:
                    print(f"Error processing flow {flow_id}: {e}")
        
        print(f"Successfully processed {successful_predictions}/{len(df_flows)} flows")
        return successful_predictions
        
    except pd.errors.EmptyDataError:
        print(f"Warning: {csv_file} was empty. No flows to process.")
        return 0
    except Exception as e:
        print(f"Error processing CSV: {e}")
        return 0

def capture_and_process_traffic():
    """Main function to capture traffic and process flows"""
    global temp_files_to_cleanup
    
    # Ensure output directories exist
    os.makedirs("Flows_test", exist_ok=True)
    os.makedirs("flow_tests_csv", exist_ok=True)
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Successfully connected to Kafka")
    except Exception as e:
        print(f"Could not connect to Kafka: {e}")
        producer = None
    
    # Check if FastAPI is running
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print("FastAPI is running and healthy")
        else:
            print("Warning: FastAPI health check failed")
    except Exception as e:
        print(f"Warning: Could not connect to FastAPI: {e}")
        print("Make sure the ML API is running on http://localhost:8000")
    
    try:
        while not stop_processing.is_set():
            timestamp = get_timestamp_str()
            pcap_file = os.path.abspath(f"Flows_test/{timestamp}.pcap")
            csv_file = os.path.abspath(f"flow_tests_csv/{timestamp}.csv")
            temp_config_file = os.path.abspath(f"temp_config_{timestamp}.json")
            
            # Add temp file to cleanup list
            temp_files_to_cleanup.append(temp_config_file)
            
            print(f"\n=== Starting new capture cycle at {timestamp} ===")
            cycle_start_time = time.time()
            
            # 1. Start traffic capture in a separate thread
            print(f"Starting traffic capture on interface {INTERFACE_NAME} for {TIME_WINDOW_SECONDS} seconds...")
            capture_thread = threading.Thread(
                target=capture_traffic,
                args=(INTERFACE_NAME, TIME_WINDOW_SECONDS, pcap_file)
            )
            capture_thread.start()
            
            # 2. While capture is running, process any previous CSV files
            if os.path.exists("flow_tests_csv"):
                csv_files = [f for f in os.listdir("flow_tests_csv") if f.endswith('.csv')]
                csv_files.sort()  # Process oldest first
                
                for csv_filename in csv_files:
                    if stop_processing.is_set():
                        break
                    
                    csv_path = os.path.join("flow_tests_csv", csv_filename)
                    if os.path.getsize(csv_path) > 0:  # Only process non-empty files
                        print(f"Processing previous CSV: {csv_filename}")
                        process_csv_file(csv_path, producer)
                        
                        # Move processed file to avoid reprocessing
                        processed_dir = "flow_tests_csv/processed"
                        os.makedirs(processed_dir, exist_ok=True)
                        try:
                            os.rename(csv_path, os.path.join(processed_dir, csv_filename))
                        except Exception as e:
                            print(f"Warning: Could not move processed file: {e}")
            
            # 3. Wait for capture to complete
            capture_thread.join()
            
            if stop_processing.is_set():
                break
            
            # 4. Process the captured traffic
            print(f"Traffic capture completed. Processing captured data...")
            
            # Create temp config for NTLFlowLyzer
            try:
                with open(CONFIG_TEMPLATE_PATH, 'r') as f:
                    config_data = json.load(f)
                config_data['pcap_file_address'] = pcap_file
                config_data['output_file_address'] = csv_file
                with open(temp_config_file, 'w') as f:
                    json.dump(config_data, f, indent=4)
                print(f"Temp config created: {temp_config_file}")
            except Exception as e:
                print(f"Error creating temp config: {e}")
                cleanup_single_temp_file(temp_config_file)
                continue
            
            # Run NTLFlowLyzer
            try:
                ntl_command = [NTL_EXECUTABLE_PATH, '-c', temp_config_file]
                subprocess.run(ntl_command, check=True)
                print(f"NTLFlowLyzer completed successfully")
            except Exception as e:
                print(f"Error running NTLFlowLyzer: {e}")
                cleanup_single_temp_file(temp_config_file)
                continue
            
            # Process the newly created CSV
            if os.path.exists(csv_file):
                process_csv_file(csv_file, producer)
            else:
                print(f"Warning: NTLFlowLyzer did not produce an output CSV file.")
            
            # Cleanup temp config file
            cleanup_single_temp_file(temp_config_file)
            
            cycle_time = time.time() - cycle_start_time
            print(f"=== Capture cycle completed in {cycle_time:.2f} seconds ===")
            
            # No sleep here - immediately start next capture cycle
            
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting gracefully.")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        cleanup_temp_files()
        if producer:
            producer.close()

def capture_traffic(interface, duration, output_file):
    """Capture traffic using tshark"""
    tshark_cmd = [
        "tshark",
        "-i", interface,
        "-a", f"duration:{duration}",
        "-w", output_file,
        "-F", "pcap"
    ]
    try:
        subprocess.run(tshark_cmd, check=True, capture_output=True)
        print(f"PCAP saved to: {output_file}")
    except Exception as e:
        print(f"Error running tshark: {e}")

# Register cleanup function to run on exit
atexit.register(cleanup_temp_files)

if __name__ == "__main__":
    try:
        capture_and_process_traffic()
    except KeyboardInterrupt:
        print("\nShutting down...")
        stop_processing.set()
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
