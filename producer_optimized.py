import os
import sys
import time
import json
import uuid
import asyncio
import aiohttp
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import signal
import atexit
import subprocess
import tempfile
import numpy as np

# Add NTLFlowLyzer to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'NTLFlowLyzer'))

from NTLFlowLyzer.network_flow_analyzer import NTLFlowLyzer
from NTLFlowLyzer.config_loader import ConfigLoader
from fluentd_logger import fluent_logger

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_data_recursively(data):
    """Recursively cleans a dictionary or list to ensure proper data types for JSON serialization."""
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = clean_data_recursively(value)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            data[i] = clean_data_recursively(item)
    elif isinstance(data, str):
        # If it's a digit string, convert to int
        if data.isdigit():
            return int(data)
        # Try converting to float
        try:
            float_val = float(data)
            # If it's a whole number, return as int
            if float_val.is_integer():
                return int(float_val)
            return float_val
        except (ValueError, TypeError):
            # If it's not a number, return the original string
            return data
    elif isinstance(data, (np.integer, np.int64)):
        return int(data)
    elif isinstance(data, (np.floating, np.float64)):
        return float(data)
    
    return data

class OptimizedDDoSProducer:
    def __init__(self, config_path: str = "configg_template.json"):
        """Initialize the optimized DDoS producer with original tshark + NTLFlowLyzer workflow."""
        self.config = self.load_config(config_path)
        self.kafka_servers = self.config.get('kafka', {}).get('servers', ['localhost:9092'])
        self.kafka_topic = self.config.get('kafka', {}).get('topic', 'processed_network_flows')
        self.ml_api_url = self.config.get('ml_api', {}).get('url', 'http://localhost:8000')
        
        # Network capture settings
        self.capture_duration = 20  # seconds
        self.network_interface = "Wi-Fi"  # or "Ethernet" based on your system
        self.pcap_output_dir = "captured_traffic"
        self.csv_output_dir = "flow_tests_csv"
        
        # Performance settings
        self.batch_size = 100
        self.max_workers = 4
        self.connection_pool_size = 50
        self.request_timeout = 60
        
        # Initialize components
        self.producer = None
        self.session = None
        self.shutdown_flag = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Initialize stats
        self.stats = {
            'flows_processed': 0,
            'batches_processed': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Initialize NTLFlowLyzer for pcap to CSV conversion
        self.flow_analyzer = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Create output directories
        os.makedirs(self.pcap_output_dir, exist_ok=True)
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        fluent_logger.log_producer_event("producer_initialized", {
            "batch_size": self.batch_size,
            "max_workers": self.max_workers,
            "connection_pool_size": self.connection_pool_size,
            "kafka_servers": self.kafka_servers,
            "kafka_topic": self.kafka_topic,
            "ml_api_url": self.ml_api_url,
            "capture_duration": self.capture_duration,
            "network_interface": self.network_interface
        })
        
        # Register cleanup
        atexit.register(self.cleanup)

    def load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return {}

    async def create_session(self):
        """Create aiohttp session with optimized settings."""
        timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=60)  # Much longer timeouts
        connector = aiohttp.TCPConnector(
            limit=100,  # Increased connection pool
            limit_per_host=50,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60
        )
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'Content-Type': 'application/json'}
        )
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='1',  # Faster acknowledgment
            batch_size=16384,  # Larger batches
            linger_ms=10,  # Wait 10ms to batch messages
            compression_type='gzip',  # Compress messages
            max_in_flight_requests_per_connection=5,
            retries=3,
            request_timeout_ms=60000  # Increased timeout
        )
        
        fluent_logger.log_producer_event("http_session_created", {
            "connection_pool_size": self.connection_pool_size,
            "timeout_seconds": self.request_timeout
        })

    async def close_session(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            fluent_logger.log_producer_event("http_session_closed", {})

    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.shutdown_flag.set()

    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        
        # Close HTTP session
        if self.session:
            try:
                # Try to close session if we're in an async context
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self.session.close())
                else:
                    # If no running loop, close synchronously
                    loop.run_until_complete(self.session.close())
            except RuntimeError:
                # No event loop, just log the cleanup
                logger.info("No event loop available for session cleanup")
            fluent_logger.log_producer_event("http_session_closed", {})
        
        # Close Kafka producer
        if self.producer:
            self.producer.close()
        
        # Shutdown executor if it exists
        if hasattr(self, 'executor') and self.executor:
            self.executor.shutdown(wait=True)
        
        # Log final stats
        fluent_logger.log_producer_event("producer_cleanup", {
            "total_flows_processed": getattr(self, 'stats', {}).get('flows_processed', 0),
            "total_batches_processed": getattr(self, 'stats', {}).get('batches_processed', 0),
            "total_errors": getattr(self, 'stats', {}).get('errors', 0),
            "uptime_seconds": time.time() - getattr(self, 'start_time', time.time())
        })

    async def process_csv_batch(self, csv_file: str, batch_size: int = None) -> None:
        """Process CSV file in optimized batches."""
        if batch_size is None:
            batch_size = self.batch_size
            
        batch_id = str(uuid.uuid4())
        start_time = time.time()
        
        try:
            # Read CSV efficiently
            csv_read_start = time.time()
            df = pd.read_csv(csv_file)
            csv_read_time = time.time() - csv_read_start
            
            total_flows = len(df)
            logger.info(f"Processing {total_flows} flows from {csv_file} in batches of {batch_size}")
            
            fluent_logger.log_producer_event("csv_processing_started", {
                "batch_id": batch_id,
                "csv_file": csv_file,
                "total_flows": total_flows,
                "batch_size": batch_size,
                "csv_read_time_seconds": csv_read_time
            })
            
            # Process in batches
            for i in range(0, total_flows, batch_size):
                if self.shutdown_flag.is_set():
                    break
                    
                batch_start = i
                batch_end = min(i + batch_size, total_flows)
                batch_df = df.iloc[batch_start:batch_end]
                
                # Convert batch to list of dictionaries
                flows = batch_df.to_dict('records')
                
                # Process batch asynchronously
                await self.process_flow_batch(flows, batch_id, i // batch_size + 1)
                
                # Log progress
                if (i // batch_size + 1) % 10 == 0:
                    elapsed = time.time() - start_time
                    flows_per_second = (i + batch_size) / elapsed
                    logger.info(f"Processed {i + batch_size}/{total_flows} flows ({flows_per_second:.1f} flows/sec)")
            
            total_time = time.time() - start_time
            flows_per_second = total_flows / total_time
            
            fluent_logger.log_producer_event("csv_processing_completed", {
                "batch_id": batch_id,
                "csv_file": csv_file,
                "total_flows": total_flows,
                "total_time_seconds": total_time,
                "flows_per_second": flows_per_second,
                "avg_time_per_flow_ms": (total_time * 1000) / total_flows
            })
            
            logger.info(f"Completed processing {total_flows} flows in {total_time:.2f}s ({flows_per_second:.1f} flows/sec)")
            
        except Exception as e:
            logger.error(f"Error processing CSV {csv_file}: {e}")
            fluent_logger.log_error("producer", f"CSV processing error: {e}", {
                "batch_id": batch_id,
                "csv_file": csv_file
            })
            self.stats['errors'] += 1

    async def process_flow_batch(self, flows: List[Dict[str, Any]], batch_id: str, batch_num: int) -> None:
        """Process a batch of flows with ML predictions and Kafka sending."""
        batch_start_time = time.time()
        
        try:
            # Get ML predictions for the entire batch
            predictions = await self.get_batch_predictions(flows)
            
            # Send to Kafka asynchronously
            kafka_tasks = []
            successful_sends = 0
            if predictions:
                for i, (flow, prediction) in enumerate(zip(flows, predictions)):
                    if prediction:
                        # Combine original flow data with prediction results
                        message_data = {**flow, **prediction}

                        # Clean the data before sending
                        cleaned_message = clean_data_recursively(message_data)

                        task = asyncio.create_task(self.send_to_kafka(cleaned_message))
                        kafka_tasks.append(task)
                
                # Wait for all Kafka sends to complete
                if kafka_tasks:
                    results = await asyncio.gather(*kafka_tasks, return_exceptions=True)
                    successful_sends = sum(1 for r in results if isinstance(r, bool) and r)


            batch_time = time.time() - batch_start_time
            
            fluent_logger.log_producer_event("batch_processed", {
                "batch_id": batch_id,
                "batch_num": batch_num,
                "flows_in_batch": len(flows),
                "successful_predictions": len([p for p in predictions if p]),
                "kafka_messages_sent": successful_sends,
                "batch_time_seconds": batch_time,
                "avg_time_per_flow_ms": (batch_time * 1000) / len(flows) if flows else 0
            })
            
            self.stats['flows_processed'] += len(flows)
            self.stats['batches_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}")
            fluent_logger.log_error("producer", f"Batch processing error: {e}", {
                "batch_id": batch_id,
                "batch_num": batch_num
            })
            self.stats['errors'] += 1

    async def get_batch_predictions(self, flows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get ML predictions for a batch of flows."""
        if not self.session:
            await self.create_session()
        
        try:
            # Prepare batch request
            batch_data = {"flows": flows}
            
            # Send batch request to ML API with retry logic
            max_retries = 2  # Reduced retries to avoid long delays
            for attempt in range(max_retries):
                try:
                    async with self.session.post(
                        f"{self.ml_api_url}/predict_batch",
                        json=batch_data,
                        headers={'Content-Type': 'application/json'}
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result.get('results', [])
                        else:
                            error_text = await response.text()
                            logger.error(f"ML API error (attempt {attempt + 1}): {response.status} - {error_text}")
                            if attempt == max_retries - 1:
                                return [None] * len(flows)
                            await asyncio.sleep(2)  # Wait before retry
                            
                except asyncio.TimeoutError:
                    logger.error(f"ML API timeout (attempt {attempt + 1})")
                    if attempt == max_retries - 1:
                        return [None] * len(flows)
                    await asyncio.sleep(3)  # Wait before retry
                    
                except Exception as e:
                    logger.error(f"ML API connection error (attempt {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        return [None] * len(flows)
                    await asyncio.sleep(2)  # Wait before retry
                    
        except Exception as e:
            logger.error(f"Error getting batch predictions: {e}")
            return [None] * len(flows)

    async def send_to_kafka(self, flow_data: Dict[str, Any]) -> None:
        """Send flow data to Kafka asynchronously."""
        try:
            # Use flow_id as key for partitioning
            flow_id = flow_data.get('flow_id', str(uuid.uuid4()))
            
            # Send to Kafka
            future = self.producer.send(self.kafka_topic, value=flow_data, key=flow_id)
            
            # Wait for send completion
            record_metadata = future.get(timeout=10)
            
            fluent_logger.log_kafka_event("message_sent", {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "flow_id": flow_id,
                "prediction": flow_data.get('prediction', 'Unknown')
            })
            
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
            fluent_logger.log_error("producer", f"Kafka send error: {e}", {
                "flow_id": flow_data.get('flow_id', 'Unknown')
            })
            self.stats['errors'] += 1

    async def capture_network_traffic(self) -> str:
        """Capture network traffic using tshark for specified duration."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        pcap_filename = f"capture_{timestamp}.pcap"
        pcap_path = os.path.join(self.pcap_output_dir, pcap_filename)
        
        logger.info(f"Starting network capture for {self.capture_duration} seconds...")
        fluent_logger.log_producer_event("capture_started", {
            "duration_seconds": self.capture_duration,
            "interface": self.network_interface,
            "pcap_file": pcap_path
        })
        
        try:
            # tshark command for live capture in PCAP format (not PCAP-NG)
            tshark_cmd = [
                "tshark",
                "-i", self.network_interface,
                "-w", pcap_path,
                "-F", "pcap",  # Force PCAP format instead of PCAP-NG
                "-a", f"duration:{self.capture_duration}",
                "-q"  # Quiet mode
            ]
            
            logger.info(f"Running tshark command: {' '.join(tshark_cmd)}")
            
            # Run tshark capture
            process = subprocess.Popen(
                tshark_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for capture to complete
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                file_size = os.path.getsize(pcap_path)
                logger.info(f"Network capture completed: {pcap_path}")
                fluent_logger.log_producer_event("capture_completed", {
                    "pcap_file": pcap_path,
                    "file_size_bytes": file_size
                })
                return pcap_path
            else:
                logger.error(f"tshark capture failed: {stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Error during network capture: {e}")
            fluent_logger.log_error("producer", f"Capture error: {e}", {})
            return None

    async def convert_pcap_to_csv(self, pcap_file: str) -> str:
        """Convert PCAP file to CSV using NTLFlowLyzer."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        csv_filename = f"flows_{timestamp}.csv"
        csv_path = os.path.join(self.csv_output_dir, csv_filename)
        
        logger.info(f"Converting PCAP to CSV: {pcap_file} -> {csv_path}")
        fluent_logger.log_producer_event("conversion_started", {
            "pcap_file": pcap_file,
            "csv_file": csv_path
        })
        
        try:
            # Create a temporary config file for this conversion
            temp_config = {
                "pcap_file_address": pcap_file,
                "output_file_address": csv_path,
                "interface_name": self.network_interface,
                "max_flow_duration": 120000,
                "activity_timeout": 5000,
                "protocols": [],
                "floating_point_unit": ".4f",
                "features_ignore_list": [],
                "number_of_threads": self.max_workers,
                "label": "Unknown",
                "feature_extractor_min_flows": 4000,
                "writer_min_rows": 6000,
                "read_packets_count_value_log_info": 10000,
                "check_flows_ending_min_flows": 2000,
                "capturer_updating_flows_min_value": 2000,
                "max_rows_number": 900000
            }
            
            # Create temporary config file
            temp_config_path = f"temp_config_{timestamp}.json"
            with open(temp_config_path, 'w') as f:
                json.dump(temp_config, f, indent=2)
            
            try:
                # Create ConfigLoader with the temporary config
                config_loader = ConfigLoader(temp_config_path)
                
                # Initialize NTLFlowLyzer with proper config
                flow_analyzer = NTLFlowLyzer(config_loader, online_capturing=False, continues_batch_mode=False)
                
                # Run the conversion
                flow_analyzer.run()
                
                # Verify CSV was created
                if os.path.exists(csv_path):
                    file_size = os.path.getsize(csv_path)
                    logger.info(f"PCAP to CSV conversion successful: {csv_path} ({file_size} bytes)")
                    fluent_logger.log_producer_event("conversion_completed", {
                        "pcap_file": pcap_file,
                        "csv_file": csv_path,
                        "file_size_bytes": file_size
                    })
                    return csv_path
                else:
                    raise Exception(f"CSV file was not created: {csv_path}")
                    
            finally:
                # Clean up temporary config file
                if os.path.exists(temp_config_path):
                    os.remove(temp_config_path)
                    
        except Exception as e:
            logger.error(f"Error converting PCAP to CSV: {e}")
            fluent_logger.log_producer_event("conversion_error", {
                "pcap_file": pcap_file,
                "error": str(e)
            })
            raise

    async def run_continuous_capture(self):
        """Main method that runs the continuous capture workflow."""
        logger.info("Starting continuous network capture workflow...")
        
        while not self.shutdown_flag.is_set():
            try:
                # Step 1: Capture network traffic with tshark
                pcap_path = await self.capture_network_traffic()
                if not pcap_path:
                    logger.error("Network capture failed, retrying in 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                
                # Step 2: Convert pcap to CSV using NTLFlowLyzer
                csv_path = await self.convert_pcap_to_csv(pcap_path)
                if not csv_path:
                    logger.error("PCAP to CSV conversion failed, retrying...")
                    continue
                
                # Step 3: Process CSV for ML predictions and send to Kafka
                await self.process_csv_batch(csv_path)
                
                # Step 4: Clean up old files (optional)
                self.cleanup_old_files()
                
                logger.info("Completed one capture cycle, starting next...")
                
            except Exception as e:
                logger.error(f"Error in capture cycle: {e}")
                fluent_logger.log_error("producer", f"Capture cycle error: {e}", {})
                await asyncio.sleep(5)

    def cleanup_old_files(self, max_files: int = 10):
        """Clean up old pcap and csv files to prevent disk space issues."""
        try:
            # Clean up old PCAP files
            pcap_files = sorted([
                os.path.join(self.pcap_output_dir, f) 
                for f in os.listdir(self.pcap_output_dir) 
                if f.endswith('.pcap')
            ])
            
            if len(pcap_files) > max_files:
                for old_file in pcap_files[:-max_files]:
                    os.remove(old_file)
                    logger.info(f"Removed old PCAP file: {old_file}")
            
            # Clean up old CSV files
            csv_files = sorted([
                os.path.join(self.csv_output_dir, f) 
                for f in os.listdir(self.csv_output_dir) 
                if f.endswith('.csv')
            ])
            
            if len(csv_files) > max_files:
                for old_file in csv_files[:-max_files]:
                    os.remove(old_file)
                    logger.info(f"Removed old CSV file: {old_file}")
                    
        except Exception as e:
            logger.error(f"Error cleaning up old files: {e}")

    async def run(self, csv_file: str = None):
        """Main run method - either process specific CSV or run continuous capture."""
        try:
            await self.create_session()
            
            if csv_file:
                # Process a specific CSV file (for testing)
                await self.process_csv_batch(csv_file)
            else:
                # Run the continuous capture workflow
                await self.run_continuous_capture()
            
        except Exception as e:
            logger.error(f"Error in main run: {e}")
            fluent_logger.log_error("producer", f"Main run error: {e}", {})
        finally:
            await self.close_session()

def main():
    """Main function for the original tshark + NTLFlowLyzer workflow."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DDoS Detection Producer - Original tshark + NTLFlowLyzer Workflow')
    parser.add_argument('--csv-file', type=str, help='Process specific CSV file (for testing)')
    parser.add_argument('--capture-duration', type=int, default=120, help='Network capture duration in seconds')
    parser.add_argument('--interface', type=str, default='Wi-Fi', help='Network interface to capture from')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker threads')
    
    args = parser.parse_args()
    
    # Create and run producer
    producer = OptimizedDDoSProducer()
    producer.capture_duration = args.capture_duration
    producer.network_interface = args.interface
    producer.batch_size = args.batch_size
    producer.max_workers = args.workers
    
    logger.info(f"Starting DDoS Detection Producer")
    logger.info(f"Capture duration: {producer.capture_duration} seconds")
    logger.info(f"Network interface: {producer.network_interface}")
    logger.info(f"Batch size: {producer.batch_size}")
    
    if args.csv_file:
        logger.info(f"Processing specific CSV file: {args.csv_file}")
    else:
        logger.info("Running continuous network capture workflow")
    
    # Run the producer
    asyncio.run(producer.run(args.csv_file))

if __name__ == "__main__":
    main() 