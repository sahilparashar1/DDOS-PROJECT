import subprocess
import time
import pandas as pd
import json
from kafka import KafkaProducer
import os

# --- Configuration ---
# All user-configurable paths and settings are at the top.
TIME_WINDOW_SECONDS = 20
# --- MODIFIED THIS LINE FOR YOUR Wi-Fi INTERFACE ---
INTERFACE_NAME = '7'
KAFKA_TOPIC = 'processed_network_flows'
NTL_EXECUTABLE_PATH = 'NTLFlowLyzer'     # Or the full path if not in your system's PATH
CONFIG_TEMPLATE_PATH = 'configg_template.json'

# --- Kafka Producer Setup ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting dynamic-config producer on interface #{INTERFACE_NAME}...")

while True:
    # 1. Generate unique filenames for this batch
    timestamp_str = str(int(time.time()))
    pcap_file = os.path.abspath(f"flows_test/capture_{timestamp_str}.pcap")
    csv_file = os.path.abspath(f"flows_tests/flows_{timestamp_str}.csv")
    temp_config_file = os.path.abspath(f"NTLFlowlyzer/config_{timestamp_str}.json")

    try:
        # 2. Capture packets into the .pcap file
        print(f"[{time.ctime()}] Capturing {TIME_WINDOW_SECONDS}s of traffic from interface #{INTERFACE_NAME}...")
        tshark_command = ['tshark', '-i', INTERFACE_NAME, '-F', 'pcap', '-a', f'duration:{TIME_WINDOW_SECONDS}', '-w', pcap_file]
        subprocess.run(tshark_command, check=True, capture_output=True)

        # 3. Dynamically create the config file for NTLFlowlyzer
        print(f"[{time.ctime()}] Creating dynamic config file: {os.path.basename(temp_config_file)}")
        with open(CONFIG_TEMPLATE_PATH, 'r') as f:
            config_data = json.load(f)

        # Update the paths in the configuration
        config_data['pcap_file_address'] = pcap_file
        config_data['output_file_address'] = csv_file

        with open(temp_config_file, 'w') as f:
            json.dump(config_data, f, indent=4)

        # 4. Run NTLFlowlyzer using the new config file
        # NOTE: The argument to specify a config file might be different (e.g., --config).
        # Adjust '-c' as needed for NTLFlowlyzer.
        print(f"[{time.ctime()}] Running NTLFlowlyzer...")
        ntl_command = [NTL_EXECUTABLE_PATH, '-c', temp_config_file]
        subprocess.run(ntl_command, check=True, capture_output=True)
        
        # 5. Read the CSV and send each flow to Kafka
        if os.path.exists(csv_file):
            print(f"[{time.ctime()}] Sending flows to Kafka...")
            try:
                df_flows = pd.read_csv(csv_file)
                
                # Replace infinite values which can occur during feature calculation
                df_flows.replace([float('inf'), float('-inf')], 0, inplace=True)
                df_flows.fillna(0, inplace=True)

                for _, row in df_flows.iterrows():
                    producer.send(KAFKA_TOPIC, row.to_dict())
                
                print(f"[{time.ctime()}] Sent {len(df_flows)} flows to Kafka topic '{KAFKA_TOPIC}'.")
            
            except pd.errors.EmptyDataError:
                print(f"[{time.ctime()}] Warning: {csv_file} was empty. No flows to send.")
            
        else:
             print(f"[{time.ctime()}] Warning: NTLFlowlyzer did not produce an output CSV file.")

    except subprocess.CalledProcessError as e:
        print(f"[{time.ctime()}] Error running an external command: {e}")
        print(f"Stderr: {e.stderr.decode()}")
    except FileNotFoundError:
        print(f"[{time.ctime()}] Error: Could not find template config file at {CONFIG_TEMPLATE_PATH}")
    except Exception as e:
        print(f"[{time.ctime()}] An unexpected error occurred: {e}")

    finally:
        # 6. Cleanup all temporary files for this batch
        print(f"[{time.ctime()}] Cleaning up temporary files...")
        if os.path.exists(pcap_file): os.remove(pcap_file)
        if os.path.exists(csv_file): os.remove(csv_file)
        if os.path.exists(temp_config_file): os.remove(temp_config_file)
        
    print(f"[{time.ctime()}] Cycle complete. Waiting for next window...")