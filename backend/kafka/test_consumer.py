"""
Kafka Test Consumer
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Sruthi (001) — Stream Processing & Feature Engineering

Reads messages from the market_trades topic and prints diagnostics.
Use to verify that the Kafka producer (csv_producer.py) is working correctly.

Usage:
  python3 backend/kafka/test_consumer.py [--topic market_trades] [--limit 100]
"""

import argparse
import json
import sys
import time
from pathlib import Path

KAFKA_BROKER = "localhost:9092"
TOPIC        = "market_trades"


def run_consumer(broker: str = KAFKA_BROKER, topic: str = TOPIC,
                 limit: int = 100, timeout_seconds: int = 30):
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("[ERROR] kafka-python not installed. Run: pip3 install kafka-python")
        sys.exit(1)

    print("=" * 60)
    print("  KAFKA TEST CONSUMER")
    print(f"  Broker : {broker}")
    print(f"  Topic  : {topic}")
    print(f"  Limit  : {limit} messages")
    print("=" * 60)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_seconds * 1000,
            group_id="test_consumer_group",
        )
    except Exception as e:
        print(f"[ERROR] Cannot connect to Kafka: {e}")
        sys.exit(1)

    received = 0
    pump_count = 0
    normal_count = 0
    symbols_seen = set()

    print(f"\nListening for up to {timeout_seconds}s …\n")

    for msg in consumer:
        data = msg.value
        received += 1
        gt = data.get("ground_truth", -1)
        sym = data.get("symbol", "?")
        symbols_seen.add(sym)

        if gt == 1:
            pump_count += 1
        elif gt == 0:
            normal_count += 1

        if received <= 5 or received % 200 == 0:
            print(f"  [{received:>5}] symbol={sym:<8} "
                  f"price={data.get('price', 0):.8f}  "
                  f"vol={data.get('volume', 0):.4f}  "
                  f"pump={gt}")

        if received >= limit:
            break

    consumer.close()
    print("\n" + "=" * 60)
    print(f"  Messages received : {received}")
    print(f"  Pump events       : {pump_count}")
    print(f"  Normal events     : {normal_count}")
    print(f"  Unique symbols    : {len(symbols_seen)}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Test Consumer")
    parser.add_argument("--broker",  default=KAFKA_BROKER)
    parser.add_argument("--topic",   default=TOPIC)
    parser.add_argument("--limit",   type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30,
                        help="Seconds to wait for messages")
    args = parser.parse_args()
    run_consumer(args.broker, args.topic, args.limit, args.timeout)
