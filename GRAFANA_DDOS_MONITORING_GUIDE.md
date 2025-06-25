# Grafana DDoS Detection Real-Time Monitoring Guide

## Overview
This guide explains how to set up and use Grafana dashboards for real-time monitoring of your DDoS detection system. The dashboards provide comprehensive visualization of network traffic patterns, ML model predictions, and security alerts.

## Dashboard Components

### 1. **Traffic Volume Over Time** 📈
- **Type**: Time Series Graph
- **Purpose**: Shows the volume of network flows being processed over time
- **Use Case**: Identify traffic spikes, patterns, and anomalies
- **Alert Threshold**: Sudden increases may indicate DDoS attacks

### 2. **Prediction Distribution** 🥧
- **Type**: Pie Chart
- **Purpose**: Visualizes the breakdown of ML predictions (Attack/Benign/Suspicious)
- **Use Case**: Quick overview of threat landscape
- **Colors**: 
  - 🔴 Red: Attacks
  - 🟡 Yellow: Suspicious
  - 🟢 Green: Benign

### 3. **Attack Detection Rate** ⚠️
- **Type**: Stat Panel
- **Purpose**: Real-time count of detected attacks
- **Thresholds**: 
  - Green: 0-9 attacks
  - Red: 10+ attacks
- **Use Case**: Immediate threat assessment

### 4. **Suspicious Activity Count** 🟡
- **Type**: Stat Panel
- **Purpose**: Count of suspicious network flows
- **Thresholds**:
  - Green: 0-4 suspicious
  - Yellow: 5-19 suspicious
  - Red: 20+ suspicious
- **Use Case**: Early warning system

### 5. **Benign Traffic Count** 🟢
- **Type**: Stat Panel
- **Purpose**: Count of normal network traffic
- **Use Case**: Baseline traffic monitoring

### 6. **Average Confidence Score** 📊
- **Type**: Stat Panel
- **Purpose**: Average confidence of ML predictions
- **Thresholds**:
  - Red: <70% confidence
  - Yellow: 70-89% confidence
  - Green: 90%+ confidence
- **Use Case**: Model performance monitoring

### 7. **Prediction Confidence Over Time** 📈
- **Type**: Time Series Graph
- **Purpose**: Tracks model confidence trends
- **Use Case**: Identify when model becomes uncertain

### 8. **Recent Predictions Table** 📋
- **Type**: Table
- **Purpose**: Detailed view of latest predictions
- **Columns**: Timestamp, Prediction, Confidence, Source IP, Destination IP
- **Use Case**: Detailed investigation of specific flows

### 9. **Top Source IPs (Attacks)** 📊
- **Type**: Bar Chart
- **Purpose**: Identifies most active attack sources
- **Use Case**: Source IP analysis and blocking decisions

### 10. **Top Destination IPs (Attacks)** 📊
- **Type**: Bar Chart
- **Purpose**: Identifies most targeted destinations
- **Use Case**: Target protection prioritization

## Setup Instructions

### 1. Start the Infrastructure
```bash
docker-compose up -d
```

### 2. Access Grafana
- **URL**: http://localhost:3001
- **Username**: admin
- **Password**: admin

### 3. Verify Data Source
- Go to Configuration → Data Sources
- Verify Elasticsearch is configured and working
- Test the connection

### 4. Access Dashboard
- Navigate to Dashboards
- Find "DDoS Detection Real-Time Monitor"
- The dashboard will auto-refresh every 5 seconds

## Data Flow Architecture

```
Network Traffic → Producer → ML API → Consumer → Elasticsearch → Grafana
     ↓              ↓         ↓         ↓           ↓           ↓
  PCAP Files → Flow Analysis → Predictions → Kafka → Storage → Visualization
```

## Key Metrics to Monitor

### High Priority Alerts
1. **Attack Count > 10**: Immediate response required
2. **Confidence Score < 70%**: Model uncertainty
3. **Traffic Spike > 200%**: Potential DDoS
4. **Suspicious Activity > 20**: Investigation needed

### Performance Metrics
1. **Average Confidence**: Should be >85%
2. **Prediction Distribution**: Should be mostly benign
3. **Response Time**: ML API should respond <1 second

## Customization Options

### Adding New Panels
1. Edit the dashboard JSON file
2. Add new panel configurations
3. Restart Grafana container

### Modifying Queries
- Use Elasticsearch query syntax
- Filter by time ranges: `$__timeFilter(@timestamp)`
- Group by intervals: `GROUP BY time($__interval)`

### Alerting Rules
1. Go to Alerting → Alert Rules
2. Create rules based on panel metrics
3. Configure notification channels

## Troubleshooting

### No Data Showing
1. Check if Elasticsearch is running: `curl http://localhost:9200`
2. Verify data is being indexed: `curl http://localhost:9200/ddos_predictions/_count`
3. Check consumer logs for errors

### Dashboard Not Loading
1. Verify Grafana is accessible: http://localhost:3001
2. Check datasource connection
3. Review Grafana logs: `docker-compose logs grafana`

### Performance Issues
1. Reduce refresh rate from 5s to 10s
2. Limit time range to last 30 minutes
3. Add query filters to reduce data volume

## Security Considerations

1. **Change Default Password**: Update admin password
2. **Network Access**: Restrict Grafana access to internal network
3. **Data Retention**: Configure Elasticsearch retention policies
4. **Audit Logs**: Enable Grafana audit logging

## Best Practices

1. **Regular Monitoring**: Check dashboard every 15 minutes
2. **Alert Configuration**: Set up email/Slack notifications
3. **Data Backup**: Regular Elasticsearch backups
4. **Performance Tuning**: Monitor system resources
5. **Documentation**: Keep runbooks for common scenarios

## Integration with Other Tools

### SIEM Integration
- Export alerts to Splunk, ELK, or QRadar
- Use Grafana webhooks for alert forwarding

### Incident Response
- Create playbooks based on dashboard metrics
- Integrate with ticketing systems

### Reporting
- Schedule dashboard exports
- Create executive summary dashboards
- Generate compliance reports 