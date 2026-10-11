"""
Kafka Dataset Replay Producer
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Sruthi (001) — Stream Processing & Feature Engineering

Reads clean events from Member 1's output (labeled_trades.csv by Nikunj)
and publishes them to a Kafka topic for Spark Structured Streaming consumption.
Falls back gracefully if Kafka is unavailable (dry-run mode).

Usage:
  python3 backend/kafka/csv_producer.py [--speed 10] [--topic market_trades]
"""

import argparse
import json
import time
import sys
from pathlib import Path

import pandas as pd

ROOT      = Path(__file__).parent.parent.parent
DATA_CSV  = ROOT / "data" / "pump_dump_dataset" / "labeled_trades.csv"

KAFKA_BROKER = "localhost:9092"
TOPIC_MARKET = "market_trades"
CHUNK_SECONDS = 5.0


def load_dataset() -> pd.DataFrame:
    if not DATA_CSV.exists():
        print(f"[ERROR] Dataset not found: {DATA_CSV}")
        print("       Run `python3 data/download.py` first.")
        sys.exit(1)
    df = pd.read_csv(DATA_CSV)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


def row_to_message(row: pd.Series, idx: int) -> dict:
    ts = row["timestamp"]
    ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
    return {
        "event_id"        : row.get("event_id", f"evt_{idx}"),
        "sequence"        : int(idx),
        "event_timestamp" : ts_str,
        "symbol"          : str(row.get("symbol", "UNK")),
        "price"           : float(row.get("price", 0)),
        "volume"          : float(row.get("volume", 0)),
        "n_trades"        : int(row.get("n_trades", 0)),
        "rush_orders"     : int(row.get("rush_orders", 0)),
        "buy_ratio"       : float(row.get("buy_ratio", 0.5)),
        "ground_truth"    : int(row.get("is_pump", 0)),
        "group"           : str(row.get("group", "unknown")),
        "exchange"        : str(row.get("exchange", "binance")),
        "source"          : "HISTORICAL_REPLAY",
    }


def run_producer(speed: float = 10.0, topic: str = TOPIC_MARKET,
                 broker: str = KAFKA_BROKER, max_rows: int = None):
    """
    Main producer loop.

    Parameters
    ----------
    speed   : replay speed multiplier (10.0 = 10× real-time)
    topic   : Kafka topic name
    broker  : Kafka bootstrap server address
    max_rows: stop after this many rows (None = full dataset)
    """
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        kafka_available = True
        print(f"  Kafka connected → {broker}")
    except Exception as e:
        print(f"  [WARN] Kafka unavailable ({e}) — dry-run mode (logging only)")
        kafka_available = False
        producer = None

    df = load_dataset()
    if max_rows:
        df = df.head(max_rows)
    total = len(df)

    print("=" * 60)
    print("  HISTORICAL DATASET REPLAY PRODUCER")
    print(f"  Dataset : SystemsLab-Sapienza Pump-and-Dump (ICCCN 2020)")
    print(f"  Records : {total:,}")
    print(f"  Topic   : {topic}")
    print(f"  Speed   : {speed}×")
    print(f"  Kafka   : {'connected' if kafka_available else 'unavailable (dry-run)'}")
    print("=" * 60)

    sleep_per_row = CHUNK_SECONDS / speed
    sent = 0
    errors = 0

    for idx, row in df.iterrows():
        msg = row_to_message(row, idx)
        try:
            if kafka_available:
                producer.send(topic, value=msg)
            else:
                # Dry run: just print
                pass
            sent += 1
            if sent % 500 == 0:
                pct = sent / total * 100
                print(f"  ▶  {sent:,}/{total:,} ({pct:.1f}%)  "
                      f"symbol={msg['symbol']}  "
                      f"pump={msg['ground_truth']}")
        except Exception as e:
            errors += 1
            print(f"  [ERROR] row {idx}: {e}")

        time.sleep(sleep_per_row)

    if kafka_available and producer:
        producer.flush()
        producer.close()

    print(f"\n  ✅ Replay complete — sent={sent:,}  errors={errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dataset Replay Producer")
    parser.add_argument("--speed",  type=float, default=10.0,
                        help="Replay speed multiplier (default: 10.0)")
    parser.add_argument("--topic",  default=TOPIC_MARKET)
    parser.add_argument("--broker", default=KAFKA_BROKER)
    parser.add_argument("--rows",   type=int, default=None,
                        help="Max rows to replay (default: all)")
    args = parser.parse_args()
    run_producer(speed=args.speed, topic=args.topic,
                 broker=args.broker, max_rows=args.rows)
