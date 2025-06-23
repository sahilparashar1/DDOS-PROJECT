# FluentD Integration for DDoS Detection System

This document explains how FluentD has been integrated into your DDoS detection system to provide centralized logging, monitoring, and observability.

## Overview

FluentD has been added to your DDoS detection system to provide:

1. **Centralized Logging**: All components now send structured logs to FluentD
2. **Real-time Monitoring**: Performance metrics and system health monitoring
3. **Data Pipeline Reliability**: Retry mechanisms and error handling
4. **Enhanced Analytics**: Comprehensive dashboards and alerting
5. **Operational Intelligence**: System performance insights and capacity planning

## Architecture

```
[Network Traffic] → [Producer] → [Kafka] → [Consumer] → [Elasticsearch]
                          ↓              ↓              ↓
                    [FluentD Agent] → [FluentD Aggregator] → [Multiple Destinations]
                                                              - Elasticsearch (logs)
                                                              - Grafana (metrics)
                                                              - Kafka (streaming)
```

## Installation

### Prerequisites

1. Docker and Docker Compose installed
2. Python 3.11+ with Poetry
3. Network interface access for tshark

### Step 1: Install Dependencies

```bash
# Install the new FluentD Python client
poetry install
```

### Step 2: Start the Infrastructure

```bash
# Start all services including FluentD
docker-compose up -d
```

This will start:
- Zookeeper
- Kafka
- Elasticsearch
- Grafana
- FluentD (with custom plugins)

### Step 3: Verify Installation

```bash
# Check if all services are running
docker-compose ps

# Check FluentD logs
docker-compose logs fluentd

# Check Elasticsearch
curl http://localhost:9200/_cluster/health
```

## Configuration

### FluentD Configuration

The FluentD configuration is located in `fluentd/conf/fluent.conf` and includes:

- **Input Sources**: Forward protocol (24224), HTTP (9880), file tailing
- **Filters**: JSON parsing, record transformation, metadata enrichment
- **Outputs**: Elasticsearch, Kafka, with buffering and retry logic

### Log Categories

The system logs are categorized as follows:

1. **ddos.producer**: Network capture and flow processing events
2. **ddos.consumer**: Kafka consumption and Elasticsearch storage
3. **ddos.ml_api**: ML prediction service events
4. **ddos.predictions**: Individual prediction results
5. **ddos.system**: System-level events and errors

## Usage

### Starting the System

1. **Start Infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Start ML API**:
   ```bash
   poetry run uvicorn ml_api:app --host 0.0.0.0 --port 8000
   ```

3. **Start Consumer** (in another terminal):
   ```bash
   poetry run python consumer.py
   ```

4. **Start Producer** (in another terminal):
   ```bash
   poetry run python producer.py
   ```

### Monitoring Dashboards

Access the monitoring dashboards:

- **Grafana**: http://localhost:3001
  - Default credentials: admin/admin
  - Pre-configured dashboard: "DDoS Detection System Monitoring"

- **Elasticsearch**: http://localhost:9200
  - Indexes: ddos-producer, ddos-consumer, ddos-ml-api, ddos-predictions

### API Endpoints

The ML API now includes additional endpoints:

- `GET /health`: System health check
- `GET /model-info`: Model information
- `POST /predict`: Flow prediction (enhanced with logging)

## Log Structure

### Producer Logs

```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "event": "batch_started",
  "level": "info",
  "component": "producer",
  "batch_id": "uuid",
  "pcap_file": "/path/to/file.pcap",
  "csv_file": "/path/to/file.csv"
}
```

### Consumer Logs

```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "event": "prediction_stored",
  "level": "info",
  "component": "consumer",
  "prediction": "Attack",
  "confidence": 0.95,
  "flow_metadata": {
    "src_ip": "192.168.1.1",
    "dst_ip": "192.168.1.2"
  },
  "storage_time_ms": 15.5
}
```

### ML API Logs

```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "event": "prediction_completed",
  "level": "info",
  "component": "ml_api",
  "prediction_id": "uuid",
  "prediction": "Attack",
  "confidence": 0.95,
  "total_time_ms": 25.3,
  "preprocessing_time_ms": 5.2,
  "prediction_time_ms": 20.1
}
```

## Performance Metrics

The system now tracks various performance metrics:

### Producer Metrics
- Traffic capture time
- NTLFlowLyzer processing time
- CSV processing time
- Total batch processing time
- Flows per second

### Consumer Metrics
- Message processing time
- Elasticsearch storage time
- Messages per second
- Error rates

### ML API Metrics
- Preprocessing time
- Prediction time
- Total processing time
- Model loading status

## Alerting and Monitoring

### Grafana Alerts

Configure alerts in Grafana for:

1. **High Error Rate**: Alert when error logs exceed threshold
2. **Low Prediction Confidence**: Alert for suspicious predictions
3. **System Performance**: Alert when processing times exceed limits
4. **Component Health**: Alert when components stop logging

### Example Alert Rules

```yaml
# High Error Rate Alert
- alert: HighErrorRate
  expr: sum(rate(ddos-*[5m])) by (level) > 0.1
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "High error rate detected"
    description: "Error rate is {{ $value }} errors per second"

# Low Prediction Confidence Alert
- alert: LowConfidencePredictions
  expr: avg(ddos-predictions{confidence < 0.7}) > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Low confidence predictions detected"
    description: "Predictions with confidence below 70%"
```

## Troubleshooting

### Common Issues

1. **FluentD Connection Failed**:
   ```bash
   # Check FluentD container status
   docker-compose logs fluentd
   
   # Check if port 24224 is accessible
   telnet localhost 24224
   ```

2. **Elasticsearch Connection Issues**:
   ```bash
   # Check Elasticsearch health
   curl http://localhost:9200/_cluster/health
   
   # Check if indexes are created
   curl http://localhost:9200/_cat/indices
   ```

3. **High Memory Usage**:
   - Check FluentD buffer settings in `fluentd/conf/fluent.conf`
   - Monitor Elasticsearch memory usage
   - Consider adjusting batch sizes

### Log Analysis

Use Elasticsearch queries to analyze logs:

```bash
# Get recent errors
curl -X GET "localhost:9200/ddos-*/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [
        {"match": {"level": "error"}},
        {"range": {"@timestamp": {"gte": "now-1h"}}}
      ]
    }
  }
}'

# Get performance metrics
curl -X GET "localhost:9200/ddos-*/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {"event": "performance_metric"}
  },
  "sort": [{"@timestamp": {"order": "desc"}}],
  "size": 100
}'
```

## Scaling Considerations

### Horizontal Scaling

1. **Multiple Producers**: Run multiple producer instances on different network interfaces
2. **Consumer Groups**: Use Kafka consumer groups for parallel processing
3. **ML API Replicas**: Deploy multiple ML API instances behind a load balancer

### Performance Tuning

1. **FluentD Buffer Settings**: Adjust buffer sizes based on log volume
2. **Elasticsearch Sharding**: Configure appropriate number of shards
3. **Kafka Partitions**: Increase topic partitions for parallel processing

## Security Considerations

1. **Network Security**: Ensure FluentD ports are properly secured
2. **Data Privacy**: Sensitive flow data is filtered in logs
3. **Access Control**: Implement proper authentication for Grafana and Elasticsearch
4. **Audit Logging**: All system events are logged for compliance

## Maintenance

### Regular Tasks

1. **Log Rotation**: Configure log rotation for FluentD logs
2. **Index Management**: Set up Elasticsearch index lifecycle management
3. **Backup**: Regular backup of Elasticsearch data
4. **Monitoring**: Monitor disk space and system resources

### Updates

1. **FluentD Updates**: Update FluentD image and plugins regularly
2. **Security Patches**: Keep all components updated
3. **Configuration Review**: Regularly review and optimize configurations

## Support

For issues and questions:

1. Check the logs in Elasticsearch or Grafana
2. Review the FluentD configuration
3. Monitor system resources and performance metrics
4. Consult the component-specific documentation

## Conclusion

The FluentD integration transforms your DDoS detection system into a production-ready, enterprise-grade security monitoring solution with comprehensive observability and operational intelligence. The centralized logging, real-time monitoring, and performance tracking provide the foundation for reliable, scalable security operations. 