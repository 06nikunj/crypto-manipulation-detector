# 🛡️ Real-Time Cryptocurrency Market Manipulation Detection

### Using Big Data Graph Streaming Analytics

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-red.svg)](https://kafka.apache.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org)
[![Neo4j](https://img.shields.io/badge/Neo4j-5.x-green.svg)](https://neo4j.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📌 About

A **real-time detection system** that identifies cryptocurrency market manipulation — including **Pump-and-Dump**, **Wash Trading**, and **Spoofing** — by fusing live exchange data with social media signals through a Big Data streaming pipeline and graph analytics.

> **Key Innovation:** First system to combine real-time streaming, multi-source data fusion, graph-based transaction analysis, and ML classification in a single unified pipeline.

---

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐
│  Binance API    │    │  Telegram API   │
│  (Live Trades)  │    │  (Pump Signals) │
└───────┬─────────┘    └───────┬─────────┘
        │                      │
        ▼                      ▼
┌──────────────────────────────────────────┐
│            APACHE KAFKA                  │
│   topic: market_trades                   │
│   topic: telegram_signals                │
└───────────────────┬──────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────┐
│         APACHE SPARK STREAMING           │
│                                          │
│   ┌──────────────────────────────────┐   │
│   │  LSTM Autoencoder → Anomaly?     │   │
│   │  XGBoost → P&D / Wash / Spoof   │   │
│   │  LightGBM → Telegram Pump?      │   │
│   └──────────────────────────────────┘   │
└──────┬───────────────────┬───────────────┘
       │                   │
       ▼                   ▼
┌─────────────┐    ┌──────────────┐
│   NEO4J     │    │  INFLUXDB    │
│  (Graph DB) │    │ (Time-Series)│
└──────┬──────┘    └──────┬───────┘
       │                  │
       └────────┬─────────┘
                ▼
       ┌────────────────┐
       │    GRAFANA      │
       │  (Dashboard)    │
       │  Live Alerts 🚨 │
       └────────────────┘
```

---

## 🤖 ML Models

| Model | Purpose | Input |
|-------|---------|-------|
| **LSTM Autoencoder** | Anomaly detection — flags abnormal trades | Price, volume, trade frequency |
| **XGBoost** | Multi-class classification — P&D / Wash Trade / Spoofing / Normal | 12 engineered features |
| **LightGBM** | Telegram pump signal detection | Message rate, keyword score, urgency |

---

## 🔍 Manipulation Types Detected

| Type | How We Detect It |
|------|-----------------|
| **Pump-and-Dump** | Price spike + volume spike + Telegram signal + hub-spoke graph pattern |
| **Wash Trading** | Circular wallet transactions (A→B→C→A) detected via Neo4j graph cycles |
| **Spoofing** | High order cancellation rate + bid-ask spread anomaly |

---

## 🛠️ Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Data Ingestion | Apache Kafka | Real-time message streaming |
| Stream Processing | Apache Spark | ML model inference on live data |
| Graph Database | Neo4j | Transaction network & cycle detection |
| Time-Series DB | InfluxDB | Metrics & alert history |
| Dashboard | Grafana | Real-time visualization & alerts |
| Language | Python 3.10+ | All backend logic |
| Containerization | Docker + Docker Compose | One-command deployment |
| Live Data | Binance WebSocket API | Exchange trade streams |
| Social Data | Telegram API | Pump group monitoring |

---

## 📁 Project Structure

```
crypto-manipulation-detector/
│
├── data/                          # Dataset links & download script
│   ├── README.md
│   └── download.py
│
├── docs/report/                   # Research paper chapters
│   ├── chapter1_introduction.md
│   ├── chapter2_literature_review.md
│   └── chapter3_methodology.md
│
├── backend/
│   ├── kafka/                     # Kafka producers & consumers
│   │   ├── binance_producer.py
│   │   ├── csv_producer.py
│   │   ├── telegram_producer.py
│   │   └── test_consumer.py
│   ├── spark/src/                 # Spark streaming + ML pipeline
│   │   ├── ingestion.py
│   │   ├── preprocessing.py
│   │   ├── anomaly_detection.py
│   │   ├── classification.py
│   │   └── graph_builder.py
│   ├── neo4j/                     # Graph DB config
│   ├── influxdb/                  # Time-series DB config
│   └── grafana/                   # Dashboard config
│
├── frontend/src/                  # Dashboard UI
│
├── scripts/                       # Training & evaluation
│   ├── train_models.py
│   ├── evaluate.py
│   └── run_pipeline.sh
│
├── docker-compose.yml             # Full stack deployment
├── requirements.txt               # Python dependencies
└── .gitignore
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Docker Desktop
- Git

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/06nikunj/crypto-manipulation-detector.git
cd crypto-manipulation-detector

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start all services
docker compose up -d

# 4. Create Kafka topics
docker exec kafka kafka-topics --create --topic market_trades --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic telegram_signals --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 5. Train ML models
python scripts/train_models.py

# 6. Start the streaming pipeline
bash scripts/run_pipeline.sh

# 7. Open Grafana dashboard
# Visit: http://localhost:3000 (admin / admin)
```

---

## 📊 Datasets Used

> ⚠️ Datasets are **not included** in this repository due to licensing. See [`data/README.md`](data/README.md) for download links.

| # | Dataset | Source | Records |
|---|---------|--------|---------|
| 1 | Elliptic Bitcoin Dataset | Kaggle | 203K transactions |
| 2 | P&D Benchmark Dataset | Zenodo | Multi-year events |
| 3 | Hu et al. Telegram P&D | GitHub (MIT) | 709 events |
| 4 | Bitcoin Heist Ransomware | UCI / Kaggle | 2.9M addresses |
| 5 | Crypto Wash Trading | Kaggle | 500K transactions |
| 6 | Binance Historical OHLCV | CryptoDataDownload | Millions |
| 7 | Ethereum NFT Wash Trading | Kaggle | NFT records |
| 8 | Mendeley Wash Trade | Mendeley Data | Labeled trades |
| 9 | La Morgia P&D Events | GitHub | Telegram + market |
| 10 | Binance Live API | Binance | Real-time stream |

---

## 📈 Expected Results

| Metric | Target |
|--------|--------|
| Accuracy | > 92% |
| Precision | > 90% |
| Recall | > 88% |
| F1 Score | > 89% |
| Detection Latency | < 2 seconds |
| Throughput | > 10,000 trades/sec |

---

## 📚 Key References

1. Kampers et al., *"Manipulation Detection in Cryptocurrency Markets"* — ACM SAC 2022
2. Hu et al., *"Sequence-Based Target Coin Prediction"* — ACM SIGMOD 2023
3. Mahrous & Di Pietro, *"PumpSense"* — IEEE ICBC 2026
4. La Morgia et al., *"The Doge of Wall Street"* — ACM Trans. Web 2023

---

## 👥 Team

| Member | Role |
|--------|------|
| Aanya | Frontend & Grafana Dashboard |
| Nikunj | Apache Kafka Backend |
| Sruthi | Apache Spark + ML Models |
| Arnav | Neo4j Graph Analytics |

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  <b>Built with ❤️ for Big Data Essentials (21CSC314P)</b>
</p>
