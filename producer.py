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
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Successfully connected to Kafka")
except Exception as e:
    print(f"Could not connect to Kafka (this is expected if you haven't installed Docker yet): {e}")
    producer = None

print("Starting PCAP to CSV conversion test...")

# Create output directories if they don't exist
os.makedirs("flows_tests_csv", exist_ok=True)

# Use the test PCAP file
test_pcap = os.path.abspath("NTLFlowLyzer/test.pcap")
csv_file = os.path.abspath("flows_tests_csv/test_output.csv")
temp_config_file = os.path.abspath("temp_config.json")

try:
    # Create the config file for NTLFlowlyzer
    print(f"Creating config file: {os.path.basename(temp_config_file)}")
    with open(CONFIG_TEMPLATE_PATH, 'r') as f:
        config_data = json.load(f)

    # Update the paths in the configuration
    config_data['pcap_file_address'] = test_pcap
    config_data['output_file_address'] = csv_file

    with open(temp_config_file, 'w') as f:
        json.dump(config_data, f, indent=4)

    # Run NTLFlowlyzer
    print(f"Running NTLFlowlyzer...")
    ntl_command = [NTL_EXECUTABLE_PATH, '-c', temp_config_file]
    subprocess.run(ntl_command, check=True)
    
    # Read the CSV and send each flow to Kafka if connected
    if os.path.exists(csv_file):
        print(f"Reading CSV file...")
        try:
            df_flows = pd.read_csv(csv_file)
            
            # Replace infinite values which can occur during feature calculation
            df_flows.replace([float('inf'), float('-inf')], 0, inplace=True)
            df_flows.fillna(0, inplace=True)

            print(f"\nSuccessfully processed {len(df_flows)} flows!")
            print(f"CSV file created at: {csv_file}")
            print(f"Number of features extracted: {len(df_flows.columns)}")
            
            # Print first few rows as example
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
        
    else:
         print(f"Warning: NTLFlowlyzer did not produce an output CSV file.")

except subprocess.CalledProcessError as e:
    print(f"Error running an external command: {e}")
    if e.stderr:
        print(f"Error output: {e.stderr.decode()}")
except FileNotFoundError as e:
    print(f"Error: Could not find a required file: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    # Cleanup temporary config file
    if os.path.exists(temp_config_file):
        os.remove(temp_config_file)
    
print("Processing complete!")
