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
    
    # Run NTLFlowLyzer and process results
    # ... rest of the code
