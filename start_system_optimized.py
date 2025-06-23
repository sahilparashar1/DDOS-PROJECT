#!/usr/bin/env python3
"""
Optimized DDoS Detection System Startup Script
Runs all components with high-performance optimizations
"""

import subprocess
import time
import signal
import sys
import os
import threading
import logging
from typing import List, Dict
import psutil
import requests
from fluentd_logger import fluent_logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('system_startup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OptimizedSystemManager:
    def __init__(self):
        self.processes: Dict[str, subprocess.Popen] = {}
        self.monitoring_thread = None
        self.shutdown_flag = threading.Event()
        
        # Performance thresholds
        self.cpu_threshold = 80.0  # CPU usage threshold
        self.memory_threshold = 85.0  # Memory usage threshold
        self.response_time_threshold = 5000  # ML API response time threshold (ms)
        
        # Component health check URLs
        self.health_checks = {
            'ml_api': 'http://localhost:8000/health',
            'elasticsearch': 'http://localhost:9200',
            'kafka': 'http://localhost:9092',
            'grafana': 'http://localhost:3001'
        }
        
        fluent_logger.log_system_event("system_manager_initialized", {
            "cpu_threshold": self.cpu_threshold,
            "memory_threshold": self.memory_threshold,
            "response_time_threshold": self.response_time_threshold
        })

    def start_docker_services(self) -> bool:
        """Start Docker services with optimized settings."""
        try:
            logger.info("Starting Docker services...")
            
            # Stop any existing containers
            subprocess.run(['docker-compose', 'down'], check=True, capture_output=True)
            
            # Start services with optimized settings
            result = subprocess.run(
                ['docker-compose', 'up', '-d'],
                check=True,
                capture_output=True,
                text=True
            )
            
            logger.info("Docker services started successfully")
            fluent_logger.log_system_event("docker_services_started", {
                "status": "success",
                "output": result.stdout
            })
            
            # Wait for services to be ready
            self.wait_for_services()
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start Docker services: {e}")
            logger.error(f"Error output: {e.stderr}")
            fluent_logger.log_error("system_manager", f"Docker services failed: {e}", {
                "error_output": e.stderr
            })
            return False

    def wait_for_services(self, timeout: int = 120) -> bool:
        """Wait for all services to be ready."""
        logger.info("Waiting for services to be ready...")
        start_time = time.time()
        
        services_ready = {
            'elasticsearch': False,
            'kafka': False,
            'ml_api': False
        }
        
        while time.time() - start_time < timeout:
            # Check Elasticsearch
            if not services_ready['elasticsearch']:
                try:
                    response = requests.get('http://localhost:9200', timeout=5)
                    if response.status_code == 200:
                        services_ready['elasticsearch'] = True
                        logger.info("✓ Elasticsearch is ready")
                except:
                    pass
            
            # Check Kafka
            if not services_ready['kafka']:
                try:
                    response = requests.get('http://localhost:9092', timeout=5)
                    if response.status_code in [200, 404]:  # Kafka returns 404 for root path
                        services_ready['kafka'] = True
                        logger.info("✓ Kafka is ready")
                except:
                    pass
            
            # Check ML API (after it's started)
            if 'ml_api' in self.processes and not services_ready['ml_api']:
                try:
                    response = requests.get('http://localhost:8000/health', timeout=5)
                    if response.status_code == 200:
                        services_ready['ml_api'] = True
                        logger.info("✓ ML API is ready")
                except:
                    pass
            
            # Check if all required services are ready
            if all(services_ready.values()):
                logger.info("All services are ready!")
                fluent_logger.log_system_event("services_ready", {
                    "elasticsearch": services_ready['elasticsearch'],
                    "kafka": services_ready['kafka'],
                    "ml_api": services_ready['ml_api'],
                    "time_taken_seconds": time.time() - start_time
                })
                return True
            
            time.sleep(2)
        
        logger.error("Timeout waiting for services to be ready")
        fluent_logger.log_error("system_manager", "Services timeout", {
            "timeout_seconds": timeout,
            "services_ready": services_ready
        })
        return False

    def start_ml_api(self):
        """Start the optimized ML API."""
        try:
            logger.info("Starting ultra-fast ML API...")
            process = subprocess.Popen([
                sys.executable, "ml_api_ultra_fast.py"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            self.processes['ml_api'] = process
            logger.info(f"ML API started with PID: {process.pid}")
            
            fluent_logger.log_system_event("ml_api_started", {
                "pid": process.pid,
                "workers": 1,
                "optimizations": [
                    "ultra_fast_batch_processing",
                    "no_logging_overhead",
                    "vectorized_operations"
                ]
            })
            
            # Wait a moment for the API to start
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"Failed to start ML API: {e}")
            fluent_logger.log_error("system_manager", f"Failed to start ML API: {e}", {})

    def start_consumer(self) -> bool:
        """Start the consumer."""
        try:
            logger.info("Starting consumer...")
            
            cmd = ['poetry', 'run', 'python', 'consumer.py']
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.processes['consumer'] = process
            logger.info(f"Consumer started with PID: {process.pid}")
            
            fluent_logger.log_system_event("consumer_started", {
                "pid": process.pid
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
            fluent_logger.log_error("system_manager", f"Consumer start failed: {e}", {})
            return False

    def start_producer(self, csv_file: str = None, batch_size: int = 50) -> bool:
        """Start the optimized producer."""
        try:
            logger.info("Starting optimized producer...")
            
            cmd = ['poetry', 'run', 'python', 'producer_optimized.py']
            if csv_file:
                cmd.extend(['--csv-file', csv_file])
            cmd.extend(['--batch-size', str(batch_size), '--workers', '4'])
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.processes['producer'] = process
            logger.info(f"Producer started with PID: {process.pid}")
            
            fluent_logger.log_system_event("producer_started", {
                "pid": process.pid,
                "csv_file": csv_file,
                "batch_size": batch_size,
                "workers": 4
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start producer: {e}")
            fluent_logger.log_error("system_manager", f"Producer start failed: {e}", {})
            return False

    def monitor_system(self):
        """Monitor system performance and component health."""
        while not self.shutdown_flag.is_set():
            try:
                # Get system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                memory_percent = memory.percent
                
                # Check component health
                health_status = {}
                for component, url in self.health_checks.items():
                    try:
                        start_time = time.time()
                        response = requests.get(url, timeout=5)
                        response_time = (time.time() - start_time) * 1000
                        
                        health_status[component] = {
                            'status': 'healthy' if response.status_code == 200 else 'unhealthy',
                            'response_time_ms': response_time,
                            'status_code': response.status_code
                        }
                        
                        # Alert if response time is too high
                        if response_time > self.response_time_threshold:
                            logger.warning(f"High response time for {component}: {response_time:.2f}ms")
                            fluent_logger.log_performance_alert("high_response_time", {
                                "component": component,
                                "response_time_ms": response_time,
                                "threshold_ms": self.response_time_threshold
                            })
                            
                    except Exception as e:
                        health_status[component] = {
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Log system metrics
                fluent_logger.log_system_metrics({
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory_percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "health_status": health_status
                })
                
                # Alert if system resources are high
                if cpu_percent > self.cpu_threshold:
                    logger.warning(f"High CPU usage: {cpu_percent:.1f}%")
                    fluent_logger.log_performance_alert("high_cpu_usage", {
                        "cpu_percent": cpu_percent,
                        "threshold": self.cpu_threshold
                    })
                
                if memory_percent > self.memory_threshold:
                    logger.warning(f"High memory usage: {memory_percent:.1f}%")
                    fluent_logger.log_performance_alert("high_memory_usage", {
                        "memory_percent": memory_percent,
                        "threshold": self.memory_threshold
                    })
                
                # Check if processes are still running
                for name, process in self.processes.items():
                    if process.poll() is not None:
                        logger.error(f"Process {name} has stopped unexpectedly")
                        fluent_logger.log_error("system_manager", f"Process {name} stopped", {
                            "process_name": name,
                            "return_code": process.returncode
                        })
                
                time.sleep(30)  # Monitor every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in system monitoring: {e}")
                time.sleep(30)

    def start_monitoring(self):
        """Start the monitoring thread."""
        self.monitoring_thread = threading.Thread(target=self.monitor_system, daemon=True)
        self.monitoring_thread.start()
        logger.info("System monitoring started")

    def stop_all(self):
        """Stop all processes gracefully."""
        logger.info("Stopping all processes...")
        self.shutdown_flag.set()
        
        for name, process in self.processes.items():
            try:
                logger.info(f"Stopping {name}...")
                process.terminate()
                
                # Wait for graceful shutdown
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Force killing {name}")
                    process.kill()
                    process.wait()
                
                logger.info(f"{name} stopped")
                
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")
        
        fluent_logger.log_system_event("system_shutdown", {
            "processes_stopped": list(self.processes.keys())
        })

    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop_all()
        sys.exit(0)

    def restart_process(self, process_name: str):
        """Restart a specific process."""
        try:
            if process_name in self.processes and self.processes[process_name]:
                self.processes[process_name].terminate()
                self.processes[process_name].wait(timeout=5)
            
            if process_name == 'ml_api':
                self.start_ml_api()
            elif process_name == 'consumer':
                self.start_consumer()
            elif process_name == 'producer':
                self.start_producer()
                
            logger.info(f"Successfully restarted {process_name}")
            fluent_logger.log_system_event("process_restarted", {
                "process_name": process_name
            })
            
        except Exception as e:
            logger.error(f"Failed to restart {process_name}: {e}")
            fluent_logger.log_error("system_manager", f"Failed to restart {process_name}", {
                "process_name": process_name,
                "error": str(e)
            })

    def monitor_processes(self):
        """Monitor all running processes and restart if needed."""
        while not self.shutdown_flag.is_set():
            try:
                for name, process in self.processes.items():
                    if process and process.poll() is not None:
                        logger.warning(f"Process {name} has stopped unexpectedly")
                        fluent_logger.log_system_event("process_stopped", {
                            "process_name": name,
                            "exit_code": process.returncode
                        })
                        
                        # Restart the process if it's critical
                        if name in ['ml_api', 'consumer']:
                            logger.info(f"Restarting {name}...")
                            self.restart_process(name)
                        else:
                            # For producer, just log the error
                            logger.error(f"Process {name} stopped - will not restart automatically")
                            fluent_logger.log_error("system_manager", f"Process {name} stopped", {
                                "process_name": name,
                                "exit_code": process.returncode
                            })
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in process monitoring: {e}")
                time.sleep(5)

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Optimized DDoS Detection System Startup')
    parser.add_argument('--csv-file', type=str, help='Specific CSV file to process')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
    parser.add_argument('--skip-docker', action='store_true', help='Skip Docker services startup')
    
    args = parser.parse_args()
    
    # Create system manager
    manager = OptimizedSystemManager()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, manager.signal_handler)
    signal.signal(signal.SIGTERM, manager.signal_handler)
    
    try:
        # Start Docker services
        if not args.skip_docker:
            if not manager.start_docker_services():
                logger.error("Failed to start Docker services")
                return 1
        
        # Start components
        if not manager.start_ml_api():
            logger.error("Failed to start ML API")
            return 1
        
        if not manager.start_consumer():
            logger.error("Failed to start consumer")
            return 1
        
        if not manager.start_producer(args.csv_file, args.batch_size):
            logger.error("Failed to start producer")
            return 1
        
        # Start monitoring
        manager.start_monitoring()
        
        logger.info("Optimized DDoS Detection System is running!")
        logger.info("Monitor performance at: http://localhost:3001 (admin/admin)")
        logger.info("Check ML API health at: http://localhost:8000/health")
        logger.info("View logs in Grafana dashboards")
        logger.info("Press Ctrl+C to stop all services")
        
        fluent_logger.log_system_event("system_started", {
            "csv_file": args.csv_file,
            "batch_size": args.batch_size,
            "skip_docker": args.skip_docker,
            "optimizations": ["batch_processing", "parallel_processing", "connection_pooling", "compression"]
        })
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        fluent_logger.log_error("system_manager", f"Unexpected error: {e}", {})
    finally:
        manager.stop_all()
        logger.info("System shutdown complete")

if __name__ == "__main__":
    sys.exit(main()) 