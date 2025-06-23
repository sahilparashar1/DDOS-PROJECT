"""
FluentD Logging Utility for DDoS Detection System
Provides centralized logging functionality for all components
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from fluent import sender

class FluentDLogger:
    """Centralized FluentD logging utility for the DDoS detection system."""
    
    def __init__(self, 
                 tag_prefix: str = "ddos",
                 host: str = "localhost", 
                 port: int = 24224,
                 timeout: float = 3.0,
                 verbose: bool = False):
        """
        Initialize FluentD logger.
        
        Args:
            tag_prefix: Prefix for all log tags
            host: FluentD host address
            port: FluentD port
            timeout: Connection timeout
            verbose: Enable verbose logging
        """
        self.tag_prefix = tag_prefix
        self.host = host
        self.port = port
        self.timeout = timeout
        self.verbose = verbose
        
        # Initialize FluentD sender
        try:
            self.sender = sender.FluentSender(tag_prefix, host=host, port=port, timeout=timeout)
            self._log_system("fluentd_connected", {"status": "success", "host": host, "port": port})
        except Exception as e:
            print(f"Warning: Could not connect to FluentD at {host}:{port}. Error: {e}")
            self.sender = None
    
    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now(timezone.utc).isoformat()
    
    def _log_system(self, event: str, data: Dict[str, Any]) -> None:
        """Log system-level events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": event,
                "level": "info",
                "component": "fluentd_logger",
                **data
            }
            self.sender.emit("system", log_data)
    
    def log_system_event(self, event: str, data: Dict[str, Any], level: str = "info") -> None:
        """Log system-level events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": event,
                "level": level,
                "component": "system",
                **data
            }
            self.sender.emit("system", log_data)
            if self.verbose:
                print(f"[SYSTEM] {event}: {json.dumps(data, indent=2)}")
    
    def log_producer_event(self, event: str, data: Dict[str, Any], level: str = "info") -> None:
        """Log producer-specific events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": event,
                "level": level,
                "component": "producer",
                **data
            }
            self.sender.emit("producer", log_data)
            if self.verbose:
                print(f"[PRODUCER] {event}: {json.dumps(data, indent=2)}")
    
    def log_consumer_event(self, event: str, data: Dict[str, Any], level: str = "info") -> None:
        """Log consumer-specific events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": event,
                "level": level,
                "component": "consumer",
                **data
            }
            self.sender.emit("consumer", log_data)
            if self.verbose:
                print(f"[CONSUMER] {event}: {json.dumps(data, indent=2)}")
    
    def log_ml_api_event(self, event: str, data: Dict[str, Any], level: str = "info") -> None:
        """Log ML API-specific events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": event,
                "level": level,
                "component": "ml_api",
                **data
            }
            self.sender.emit("ml_api", log_data)
            if self.verbose:
                print(f"[ML_API] {event}: {json.dumps(data, indent=2)}")
    
    def log_kafka_event(self, event: str, data: Dict[str, Any], level: str = "info") -> None:
        """Log Kafka-specific events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": event,
                "level": level,
                "component": "kafka",
                **data
            }
            self.sender.emit("kafka", log_data)
            if self.verbose:
                print(f"[KAFKA] {event}: {json.dumps(data, indent=2)}")
    
    def log_prediction(self, 
                      flow_id: str,
                      prediction: str, 
                      confidence: float, 
                      all_probabilities: Dict[str, float],
                      processing_time_ms: float,
                      flow_data: Optional[Dict[str, Any]] = None) -> None:
        """Log ML prediction results."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "flow_id": flow_id,
                "prediction": prediction,
                "confidence": confidence,
                "all_probabilities": all_probabilities,
                "processing_time_ms": processing_time_ms,
                "component": "ml_api",
                "event": "prediction_made"
            }
            
            # Add flow metadata if provided (without sensitive data)
            if flow_data:
                # Extract non-sensitive flow metadata
                flow_metadata = {
                    "src_ip": flow_data.get("src_ip", "unknown"),
                    "dst_ip": flow_data.get("dst_ip", "unknown"),
                    "src_port": flow_data.get("src_port", "unknown"),
                    "dst_port": flow_data.get("dst_port", "unknown"),
                    "protocol": flow_data.get("protocol", "unknown"),
                    "duration": flow_data.get("duration", 0),
                    "packets_count": flow_data.get("packets_count", 0),
                    "total_payload_bytes": flow_data.get("total_payload_bytes", 0)
                }
                log_data["flow_metadata"] = flow_metadata
            
            self.sender.emit("predictions", log_data)
            if self.verbose:
                print(f"[PREDICTION] {prediction} (Confidence: {confidence:.4f}) for flow {flow_id}")
    
    def log_error(self, component: str, error: str, details: Dict[str, Any] = None) -> None:
        """Log error events."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": "error",
                "level": "error",
                "component": component,
                "error_message": error,
                "details": details or {}
            }
            self.sender.emit("system", log_data)
            if self.verbose:
                print(f"[ERROR] {component}: {error}")
    
    def log_performance(self, component: str, metric: str, value: float, unit: str = "ms") -> None:
        """Log performance metrics."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": "performance_metric",
                "level": "info",
                "component": component,
                "metric": metric,
                "value": value,
                "unit": unit
            }
            self.sender.emit("system", log_data)
            if self.verbose:
                print(f"[PERFORMANCE] {component} {metric}: {value} {unit}")
    
    def log_performance_alert(self, alert_type: str, data: Dict[str, Any]) -> None:
        """Log performance alerts."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": "performance_alert",
                "level": "warning",
                "component": "system",
                "alert_type": alert_type,
                **data
            }
            self.sender.emit("alerts", log_data)
            if self.verbose:
                print(f"[PERFORMANCE_ALERT] {alert_type}: {json.dumps(data, indent=2)}")
    
    def log_system_metrics(self, metrics: Dict[str, Any]) -> None:
        """Log system metrics."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": "system_metrics",
                "level": "info",
                "component": "system",
                **metrics
            }
            self.sender.emit("metrics", log_data)
            if self.verbose:
                print(f"[SYSTEM_METRICS] {json.dumps(metrics, indent=2)}")
    
    def log_flow_processing(self, 
                           flow_count: int, 
                           processing_time: float, 
                           success_count: int = None,
                           error_count: int = None) -> None:
        """Log flow processing statistics."""
        if self.sender:
            log_data = {
                "timestamp": self._get_timestamp(),
                "event": "flow_processing_batch",
                "level": "info",
                "component": "producer",
                "flow_count": flow_count,
                "processing_time_seconds": processing_time,
                "flows_per_second": flow_count / processing_time if processing_time > 0 else 0
            }
            
            if success_count is not None:
                log_data["success_count"] = success_count
            if error_count is not None:
                log_data["error_count"] = error_count
            
            self.sender.emit("producer", log_data)
            if self.verbose:
                print(f"[FLOW_PROCESSING] Processed {flow_count} flows in {processing_time:.2f}s")
    
    def close(self) -> None:
        """Close the FluentD connection."""
        if self.sender:
            self.sender.close()
            self._log_system("fluentd_disconnected", {"status": "success"})

# Global instance
fluent_logger = FluentDLogger(verbose=True) 