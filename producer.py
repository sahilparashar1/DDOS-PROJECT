import subprocess
import time
import pandas as pd
import json
from kafka import KafkaProducer
import os
import datetime
import sys

# --- Configuration ---
TIME_WINDOW_SECONDS = 120
INTERFACE_NAME = '8'  # Change as needed
KAFKA_TOPIC = 'processed_network_flows'
NTL_EXECUTABLE_PATH = 'NTLFlowLyzer'  # Or the full path if not in your system's PATH
CONFIG_TEMPLATE_PATH = 'configg_template.json'

def get_timestamp_str():
    return datetime.datetime.now().strftime("%d_%m_%y_%H_%M_%S")

# Ensure output directories exist
os.makedirs("Flows_test", exist_ok=True)
os.makedirs("flow_tests_csv", exist_ok=True)

try:
    # Create Kafka producer ONCE
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Successfully connected to Kafka")
except Exception as e:
    print(f"Could not connect to Kafka (this is expected if you haven't installed Docker yet): {e}")
    producer = None
try:
    while True:
        timestamp = get_timestamp_str()
        pcap_file = os.path.abspath(f"Flows_test/{timestamp}.pcap")
        csv_file = os.path.abspath(f"flow_tests_csv/{timestamp}.csv")
        temp_config_file = os.path.abspath(f"temp_config_{timestamp}.json")

        # 1. Capture live traffic with tshark
        print(f"\nCapturing live traffic on interface {INTERFACE_NAME} for {TIME_WINDOW_SECONDS} seconds...")
        tshark_cmd = [
            "tshark",
            "-i", INTERFACE_NAME,
            "-a", f"duration:{TIME_WINDOW_SECONDS}",
            "-w", pcap_file,
            "-F", "pcap"
        ]
        try:
            start_time=time.time()
            subprocess.run(tshark_cmd, check=True)
            print(f"PCAP saved to: {pcap_file}")
        except Exception as e:
            print(f"Error running tshark: {e}")
            continue  # Skip to next iteration

        # 2. Create temp config for NTLFlowLyzer
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
            continue

        # 3. Run NTLFlowLyzer
        try:
            ntl_command = [NTL_EXECUTABLE_PATH, '-c', temp_config_file]
            subprocess.run(ntl_command, check=True)
        except Exception as e:
            print(f"Error running NTLFlowLyzer: {e}")
            continue

        # 4. Read the CSV and send each flow to Kafka if connected
        if os.path.exists(csv_file):
            print(f"Reading CSV file: {csv_file}")
            try:
                df_flows = pd.read_csv(csv_file)
                # Clean the raw data before sending to Kafka
                df_flows = df_flows.replace([float('inf'), float('-inf')], 0)
                df_flows = df_flows.fillna(0)
                df_flows = df_flows.replace({'handshake_duration': 'not a complete handshake'}, -1)
                df_flows['handshake_duration'] = pd.to_numeric(df_flows['handshake_duration'], errors='coerce').fillna(-1)

                print(f"\nSuccessfully processed {len(df_flows)} flows!")
                print(f"CSV file created at: {csv_file}")
                print(f"Number of features extracted: {len(df_flows.columns)}")
                print("\nFirst few rows of the data:")
                print(df_flows.head().to_string())

                if producer:
                    print("\nSending flows to Kafka...")
                    for _, row in df_flows.iterrows():
                        producer.send(KAFKA_TOPIC, row.to_dict())
                    print(f"Sent {len(df_flows)} flows to Kafka topic '{KAFKA_TOPIC}'.")
                else:
                    print("\nSkipping Kafka sending (no connection)")

            except pd.errors.EmptyDataError:
                print(f"Warning: {csv_file} was empty. No flows to process.")
            except Exception as e:
                print(f"Error processing CSV: {e}")
        else:
            print(f"Warning: NTLFlowLyzer did not produce an output CSV file.")

        # 5. Cleanup temp config file
        if os.path.exists(temp_config_file):
            os.remove(temp_config_file)

        print("Processing complete! Waiting before next capture...\n")
        end_time=time.time()
        print(f"Time taken: {end_time-start_time} seconds")
        #time.sleep(2)  # Optional: short pause before next capture

except KeyboardInterrupt:
    print("\nProgram interrupted by user. Exiting gracefully.")
    sys.exit(0)
except Exception as e:
    print(f"Fatal error: {e}")
    sys.exit(1)
