# test_pcap_to_csv.py
import os
import subprocess
import json
import pandas as pd

def ensure_directories():
    """Create necessary directories if they don't exist"""
    dirs = ['Flows_test', 'flows_tests_csv']
    for dir_name in dirs:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"Created directory: {dir_name}")

def test_pcap_conversion():
    # Create directories
    ensure_directories()
    
    # Use the test.pcap file
    test_pcap = "NTLFlowLyzer/test.pcap"
    output_csv = "flows_tests_csv/test_output.csv"
    
    # Create a temporary config file
    temp_config = "temp_config.json"
    with open("configg_template.json", 'r') as f:
        config_data = json.load(f)
    
    # Update paths in config
    config_data['pcap_file_address'] = os.path.abspath(test_pcap)
    config_data['output_file_address'] = os.path.abspath(output_csv)
    with open(temp_config, 'w') as f:
        json.dump(config_data, f, indent=4)
   #Running NTLFlowLyzer
    print(f"Running NTLFlowlyzer...")
    ntl_command = ['NTLFlowLyzer','-c','temp_config.json'] 
    subprocess.run(ntl_command, check=True)

    #Reading the output CSV
    print(f"Reading the output CSV...")
    df=pd.read_csv(output_csv)
    print(f"CSV file read successfully!")

    #Displaying the first few rows of the dataframe
    print(f"First few rows of the dataframe:")
    print(df.head())

if __name__ == "__main__":
    test_pcap_conversion()
    #Removing the temporary config file
    if os.path.exists('temp_config.json'):
        os.remove('temp_config.json')
    print("Temporary config file removed!")
