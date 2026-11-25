# 🛒 Real-Time E-Commerce Big Data & Lakehouse Platform

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B-E25A1C.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-231F20.svg)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestrated%20DAGs-017CEE.svg)](https://airflow.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B.svg)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20Ready-2496ED.svg)](https://www.docker.com/)

A production-style, dual-engine (Batch + Real-Time) Lakehouse platform designed to ingest, process, model, and visualize high-velocity retail transactions and live customer clickstream events at enterprise scale.

---

## 🏛️ System Architecture

```
                                  [DATA SOURCES]
                 ┌──────────────────────────────────────────────┐
                 │  Historical Retail (541K Transactions)        │
                 │  Synthetic Live Clickstream (Kafka Producer) │
                 └──────────────────────┬───────────────────────┘
                                        │
           ┌────────────────────────────┴────────────────────────────┐
           ▼                                                         ▼
   [BATCH LAKEHOUSE]                                        [STREAMING PIPELINE]
┌─────────────────────────┐                             ┌─────────────────────────┐
│ PySpark Bronze ETL      │                             │ Apache Kafka Broker     │
│ • Filter 'C' / 'A' inv. │                             │ • Topic: customer-events│
│ • Schema Enforcement    │                             │ • Multi-partition log   │
│ • Outlier/Null Pruning  │                             └───────────┬─────────────┘
└──────────┬──────────────┘                                         │
           ▼                                                        ▼
┌─────────────────────────┐                             ┌─────────────────────────┐
│ Silver Conformed Store  │                             │ Spark Structured Stream │
│ • Cleaned Parquet       │                             │ • Event-time Watermark  │
│ • Partitioned (Year/Mo) │                             │ • Schema Deserialization│
└──────────┬──────────────┘                             │ • Checkpointed Sink     │
           │                                            └───────────┬─────────────┘
           ▼                                                        │
┌─────────────────────────┐                                         │
│ Gold Star Schema Models │                                         ▼
│ • dim_customer (RFM)    │                             ┌─────────────────────────┐
│ • dim_product           │                             │ Bronze Streaming Store  │
│ • fact_sales_monthly    │                             │ • Partitioned Parquet   │
│ • Bipartite Graph Graph │                             └───────────┬─────────────┘
└──────────┬──────────────┘                                         │
           │                                                        │
           └────────────────────────────┬───────────────────────────┘
                                        ▼
                         [PRESENTATION & CONSUMPTION]
                 ┌──────────────────────────────────────────────┐
                 │ Streamlit Executive Intelligence Dashboard   │
                 │ • Historical Sales & Revenue Trends          │
                 │ • RFM Customer Clusters (VIP/Active/At-Risk) │
                 │ • Model Benchmarks (Regression/Classification│
                 │ • Customer-Product Network Graph             │
                 │ • Real-Time Kafka Stream Event Monitor       │
                 └──────────────────────────────────────────────┘
```

---

## 📦 Key Highlights & Engineering Metrics

- **Medallion Lakehouse ETL**: Ingests **541,909 raw transactions**, enforcing data quality invariants to isolate 144K+ invalid/cancelled rows, persisting **397,884 clean records** (£8.91M net sales) into partitioned Silver and Gold Parquet stores.
- **Fault-Tolerant Real-Time Stream**: Ingests live multi-action events (`view`, `add_to_cart`, `purchase`, `cancel`) via **Spark Structured Streaming** with 10-minute event-time watermarking and write-ahead metadata checkpointing.
- **Dimensional Star Schema & Graph Modeling**: Built `dim_customer` (RFM features), `dim_product`, and an **8,003-node bipartite network graph** (4,338 customer nodes, 3,665 product nodes, 266,792 edges) via NetworkX to detect purchasing hubs and co-purchase affinities.
- **Customer Segmentation & Predictive AI**:
  - **RFM Clustering**: Partitioned customers into 3 distinct cohorts (VIP Whales, Active Shoppers, At-Risk) with a **Silhouette score of 0.7138**.
  - **90-Day Churn Prediction**: Logistic Regression achieving **66.67% accuracy** and **67.26% precision**.
  - **CLV Spending Prediction**: PySpark Linear Regression explaining **59.23% variance ($R^2$)** in future 90-day spend.
- **Workflow Orchestration**: Automated end-to-end DAG defined in **Apache Airflow** (`dags/ecommerce_etl_dag.py`) with retries, dependency graphs, and health checks.
- **Single-Command Orchestration**: Unified CLI runner (`run_pipeline.py`) and Docker Compose environment (`docker-compose.yml`) for zero-friction setup.

---

## 🛠️ Tech Stack

| Category | Tools & Libraries |
|---|---|
| **Distributed Compute** | Apache Spark 3.5+, PySpark (SQL, Streaming, MLlib) |
| **Message Streaming** | Apache Kafka, Kafka-Python, Confluent Docker Images |
| **Workflow Orchestration** | Apache Airflow (`dags/ecommerce_etl_dag.py`) |
| **Storage & Data Lake** | Apache Parquet (Snappy-compressed), CSV, Metadata Checkpoints |
| **Machine Learning & Graph** | Scikit-Learn, SciPy, NetworkX, NumPy, Pandas |
| **Dashboard & UI** | Streamlit, Matplotlib |
| **DevOps & Testing** | Docker, Docker Compose, Unittest, Pytest, PyYAML |

---

## 📂 Project Directory Structure

```
Ecommerce-BigData-Analytics/
├── config/
│   ├── config.yaml                 # Centralized environment, paths, Kafka & Spark settings
│   └── settings.py                 # Dynamic configuration loader and path resolver
├── dags/
│   └── ecommerce_etl_dag.py        # Apache Airflow workflow DAG definition
├── dashboard/
│   └── app.py                      # Multi-tab Streamlit Executive Dashboard
├── data/
│   ├── raw/                        # Ingestion raw datasets (Online Retail.xlsx)
│   ├── processed/                  # Silver & Gold conformed Parquet tables
│   └── streaming/                  # Parquet streaming sink & write-ahead checkpoint logs
├── docker-compose.yml              # Local Kafka, Zookeeper & Kafka UI orchestration
├── graph/
│   └── customer_product_graph.py   # Bipartite network graph generation & centrality
├── kafka/
│   └── producer.py                 # Real-time synthetic clickstream event publisher
├── ml/
│   ├── feature_engineering.py      # RFM vector calculation & StandardScaler
│   ├── hierarchical_clustering.py  # Ward linkage agglomerative clustering & dendrogram
│   ├── kmeans_clustering.py        # K-Means clustering with WCSS & Silhouette evaluation
│   ├── linear_regression.py        # PySpark CLV spending prediction
│   ├── logistic_regression.py      # PySpark 90-day churn classification
│   └── model_comparison.py         # Evaluation benchmark exporter
├── results/                        # PNG charts, linkage matrices, and model metrics CSV
├── spark/
│   ├── eda.py                      # PySpark exploratory analysis
│   └── etl_pipeline.py             # Modular Medallion PySpark ETL pipeline
├── streaming/
│   ├── spark_kafka_consumer.py     # Spark Structured Streaming Kafka consumer with watermarking
│   └── verify_stream.py            # Streaming health auditor & partition inspector
├── tests/
│   └── test_etl_pipeline.py        # Automated test suite (cleaning, schemas, RFM invariants)
├── utils/
│   └── logger.py                   # Centralized structured logging formatter
├── run_pipeline.py                 # Unified master CLI orchestrator
├── requirements.txt                # Pinned Python dependencies
└── README.md                       # Documentation & Architecture Overview
```

---

## 🚀 Quickstart Guide

### 1. Environment Setup
```bash
# Clone the repository
git clone https://github.com/your-username/Ecommerce-BigData-Analytics.git
cd Ecommerce-BigData-Analytics

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Launch Local Kafka Infrastructure
```bash
# Spin up Kafka, Zookeeper, and Kafka UI with one command
docker compose up -d

# Open Kafka UI in your browser to inspect topics and partitions:
# http://localhost:8085
```

### 3. Run Pipeline via Master CLI Runner
The repository includes `run_pipeline.py` to trigger individual or end-to-end components:

```bash
# Run automated data quality and invariant tests
python run_pipeline.py --step test

# Execute PySpark Medallion ETL pipeline (Bronze -> Silver -> Gold)
python run_pipeline.py --step etl

# Ingest simulated clickstream events into Kafka
python run_pipeline.py --step producer --events 50

# Start Spark Structured Streaming consumer
python run_pipeline.py --step consumer

# Audit streaming Parquet partition health
python run_pipeline.py --step verify-stream

# Generate customer-product bipartite graph
python run_pipeline.py --step graph

# Launch executive Streamlit dashboard
python run_pipeline.py --step dashboard
```

---

## 🧪 Data Invariants & Quality Rules

1. **Transaction Validity**: Records with invoice numbers starting with `'C'` (cancellations) or `'A'` (bad debts) are systematically filtered out of revenue metrics.
2. **Numeric Boundaries**: `Quantity > 0` and `UnitPrice > 0` constraints prevent distorted margins.
3. **Identity Resolution**: Missing `CustomerID` records are isolated from customer-level RFM modeling while retained for SKU catalog aggregation.
4. **Streaming Resilience**: Spark Structured Streaming utilizes metadata checkpointing and 10-minute watermarking to handle out-of-order records and prevent duplicate writes.

---

## 🔮 Future Roadmap
- **Delta Lake Integration**: Migrate Parquet storage to Delta Lake for ACID transactions, schema enforcement, and time-travel querying.
- **Cloud Scale**: Deploy Kafka onto **Amazon MSK** and PySpark jobs onto **Databricks / AWS EMR**.
