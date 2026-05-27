# Real-Time E-Commerce Clickstream Pipeline
 
[![CI Pipeline](https://github.com/Mohit-03Sharma/clickstream-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Mohit-03Sharma/clickstream-pipeline/actions/workflows/ci.yml)
 
End-to-end real-time data pipeline processing e-commerce clickstream events using Apache Kafka, Spark Structured Streaming, and Apache Airflow — with anomaly detection, batch session analytics, and a live Streamlit dashboard.
 
---
 
## Architecture
 
```
Event Simulator (Python + Faker)
        │
        ▼
Kafka Topics + Confluent Schema Registry (Avro)
        │
        ├──▶ Spark Structured Streaming ──▶ Windowed Aggregations
        │           │                              │
        │           ▼                              ▼
        │    Anomaly Detectors              Console / BigQuery
        │    (traffic spike,
        │     conversion drop,
        │     abandonment surge)
        │
        └──▶ Airflow Hourly DAG ──▶ Session Summaries + Funnel
                                            │
                                            ▼
                                   Streamlit Dashboard
```
 
---
 
## Key Metrics
 
| Component | Detail |
|---|---|
| Event throughput | 10–100+ events/second (configurable) |
| Event types | 5 (page_view, product_click, add_to_cart, purchase, session_end) |
| Kafka topics | 5 with Avro schema enforcement via Schema Registry |
| Streaming aggregations | Page views per minute (tumbling), cart rate (sliding 2min/30s), purchase conversion (sliding 5min/1min) |
| Anomaly detectors | 3 (traffic spike, conversion drop, cart abandonment surge) |
| Batch pipeline | Hourly Airflow DAG: session summaries, funnel drop-off rates, data quality checks |
| Unit tests | 28 passing |
| CI/CD | GitHub Actions: pytest + flake8 on every push |
 
---
 
## Tech Stack
 
| Category | Tools |
|---|---|
| Event Streaming | Apache Kafka (Confluent Platform), Confluent Schema Registry, Avro |
| Stream Processing | PySpark Structured Streaming 3.5.0 |
| Orchestration | Apache Airflow 2.10.3 |
| Dashboard | Streamlit, Plotly |
| Infrastructure | Docker, Docker Compose, GitHub Actions CI/CD |
| Language | Python 3.12 |
 
---
 
## Project Structure
 
```
clickstream-pipeline/
├── producer/
│   ├── event_simulator.py        # Generates and publishes events to Kafka
│   ├── verify_consumer.py        # Reads and prints events from Kafka
│   └── schemas/                  # Avro schema definitions (5 event types)
├── streaming/
│   ├── spark_consumer.py         # Spark Structured Streaming with windowed aggregations
│   └── anomaly_detector.py       # Three streaming anomaly detectors
├── dags/
│   ├── hourly_summary.py         # Airflow DAG definition
│   └── run_hourly_summary.py     # Standalone runner (Windows-compatible)
├── dashboard/
│   └── app.py                    # Streamlit dashboard (4 pages)
├── tests/
│   ├── test_producer.py          # Unit tests for event schema and weights
│   └── test_anomaly.py           # Unit tests for anomaly detection thresholds
├── docker-compose.yml            # Kafka, Zookeeper, Schema Registry, Kafka UI
└── .github/workflows/ci.yml      # GitHub Actions: pytest + flake8 on every push
```
 
---
 
## Setup
 
**Prerequisites:** Docker Desktop, Python 3.12
 
**1. Start Kafka environment**
```bash
docker compose up -d
```
 
**2. Create virtual environment and install dependencies**
```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```
 
**3. Start the event simulator**
```bash
python producer/event_simulator.py
```
 
**4. Run Spark Structured Streaming**
```bash
python streaming/spark_consumer.py
```
 
**5. Run anomaly detectors**
```bash
python streaming/anomaly_detector.py
```
 
**6. Run hourly batch pipeline**
```bash
python dags/run_hourly_summary.py
```
 
**7. Launch Streamlit dashboard**
```bash
streamlit run dashboard/app.py
```
 
Open `http://localhost:8501` for the dashboard and `http://localhost:8080` for Kafka UI.
 
---
 
## Pipeline Components
 
### Event Simulator
Generates realistic e-commerce events with configurable funnel ratios: 70% page views, 15% product clicks, 10% cart adds, 4% purchases, 1% session ends. Uses a pool of 50 simulated users with rotating sessions. Events are Avro-serialized and published to Kafka with schema enforcement via Confluent Schema Registry.
 
### Spark Structured Streaming
Three windowed aggregations running continuously:
- Page views per minute per page (tumbling window)
- Add-to-cart rate per 2-minute window sliding every 30 seconds
- Purchase conversion rate per 5-minute window sliding every minute
### Anomaly Detection
Three detectors running on the live stream:
- **Traffic spike** — flags 1-minute windows exceeding the baseline threshold
- **Conversion drop** — flags 5-minute windows with purchase counts below 40% of baseline
- **Cart abandonment surge** — flags 5-minute windows with cart events above 1.5x baseline
### Airflow Batch DAG
Five-task hourly pipeline: extract events → compute session summaries → compute funnel → write outputs → data quality checks. Computes session-level metrics (duration, pages viewed, conversion, total spend) that require seeing complete sessions — impossible in the streaming layer.
 
### Streamlit Dashboard
Four pages: Live Event Feed (Kafka topic message counts), Funnel Analysis (conversion funnel from batch output), Session Insights (duration distribution, conversion split), Pipeline Health (Kafka broker, Schema Registry, batch run status).
 
---
 
## Running Tests
 
```bash
pytest tests/ -v
```
 
28 unit tests covering event schema validation, funnel weight logic, and anomaly detection thresholds. No Kafka or Docker required.
 
---