# Real-time DDoS Detection System

This project implements a real-time DDoS detection system using a machine learning model. It captures live network traffic, processes it to extract features, predicts the likelihood of a DDoS attack for each network flow, and provides a dashboard for visualizing the results.

## Project Overview

The system is composed of the following main components:

1.  **Traffic Capture and Feature Extraction (`producer.py` + `NTLFlowLyzer`)**: 
    *   A Python script (`producer.py`) continuously captures live network traffic from a chosen network interface using `tshark`.
    *   It utilizes the `NTLFlowLyzer` tool to process the captured traffic (`.pcap` files) and extract a comprehensive set of flow-based features.
    *   `NTLFlowLyzer` is a highly configurable Python package designed for network traffic analysis.

2.  **Machine Learning API (`ml_api.py`)**:
    *   A FastAPI server that serves a pre-trained Logistic Regression model (`LR_model_artifacts.pkl`).
    *   It exposes a `/predict` endpoint that takes flow features as input and returns a prediction (e.g., "Attack", "Benign", "Suspicious") along with a confidence score.

3.  **Data Pipeline (`producer.py`, `Kafka`, `consumer.py`)**:
    *   The `producer.py` script sends the extracted flow features to the `ml_api.py` for prediction.
    *   It then publishes the flow data, enriched with the prediction results, to a Kafka topic named `processed_network_flows`.
    *   A `consumer.py` script subscribes to this Kafka topic, consumes the enriched data, and stores it in Elasticsearch for analysis and visualization.

4.  **Monitoring and Visualization (`Elasticsearch`, `Grafana`)**:
    *   The processed data, including predictions, is stored in an Elasticsearch index.
    *   A Grafana instance is pre-configured with a dashboard to visualize the data from Elasticsearch, enabling real-time monitoring of network traffic and potential DDoS attacks.

5.  **Orchestration (`docker-compose.yml`)**:
    *   A `docker-compose.yml` file is provided to easily set up the necessary backend infrastructure, which includes Zookeeper, Kafka, Elasticsearch, and Grafana.

## Architecture

The overall architecture of the system can be visualized as follows:

`Live Traffic` -> `tshark` -> `.pcap file` -> `NTLFlowLyzer` -> `.csv file` -> `producer.py` -> `ml_api.py` -> `Kafka` -> `consumer.py` -> `Elasticsearch` -> `Grafana`

A more detailed diagram of the feature extraction component (`NTLFlowLyzer`) is available at `NTLFlowLyzer/Architecture.svg`.

## How to Run the Project

To run the complete system, follow these steps:

### Prerequisites

*   Python 3.8+
*   Docker and Docker Compose
*   Wireshark (ensure `tshark` is in your system's PATH)
*   Python dependencies. Install them using `pip install -r requirements.txt`. The `NTLFlowLyzer` has its own `requirements.txt` as well.

### 1. Install NTLFlowLyzer

Navigate to the `NTLFlowLyzer` directory and install the package.

```bash
cd NTLFlowLyzer
pip install .
cd ..
```

### 2. Start the Backend Infrastructure

Start the backend services (Kafka, Zookeeper, Elasticsearch, Grafana) using Docker Compose.

```bash
docker-compose up -d
```

You can check the status of the containers with `docker-compose ps`.

### 3. Run the Machine Learning API

In a new terminal, start the ML API server.

```bash
python ml_api.py
```

The API will be available at `http://localhost:8000`. You can check its health at `http://localhost:8000/health`.

### 4. Run the Consumer

In another terminal, start the Kafka consumer to store results in Elasticsearch.

```bash
python consumer.py
```

### 5. Run the Producer

Finally, in a separate terminal, start the producer to capture traffic and begin the detection process.

**Note:** You may need to run this script with `sudo` on Linux/macOS or as an administrator on Windows to allow for network interface capture. You might also need to change the `INTERFACE_NAME` variable in `producer.py` to match the network interface you want to monitor (e.g., 'eth0', 'en0').

```bash
python producer.py
```

### 6. View the Dashboard

Open your web browser and navigate to Grafana at `http://localhost:3001`.

*   **Username**: admin
*   **Password**: admin

You should find a pre-configured dashboard named "DDoS Monitoring" that visualizes the predictions and network flow data from Elasticsearch.

## Project Structure

*   `producer.py`: Captures traffic, uses NTLFlowLyzer, gets predictions, and sends data to Kafka.
*   `consumer.py`: Consumes data from Kafka and stores it in Elasticsearch.
*   `ml_api.py`: FastAPI application to serve the ML model.
*   `LR_model_artifacts.pkl`: Pre-trained machine learning model and scaler.
*   `docker-compose.yml`: Defines and configures the backend services.
*   `NTLFlowLyzer/`: A self-contained Python package for feature extraction from network traffic.
*   `grafana/`: Contains Grafana dashboard and datasource configurations.
*   `data/`: Directory for datasets.
*   `configg_template.json`: Template configuration for `NTLFlowLyzer`.

## Customization

*   **Network Interface**: Change the `INTERFACE_NAME` in `producer.py` to the desired network interface.
*   **Capture Window**: Adjust the `TIME_WINDOW_SECONDS` in `producer.py` to change the capture duration for each cycle.
*   **ML Model**: The `ml_api.py` can be modified to load a different model. The model and associated artifacts are stored in `LR_model_artifacts.pkl`.
*   **`NTLFlowLyzer` Configuration**: The `configg_template.json` file can be modified to change the behavior of `NTLFlowLyzer`, such as which features to extract. See the `NTLFlowLyzer/README.md` for more details.
