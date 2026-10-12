"""Optional bridge: Binance public Spot trades -> Kafka.

The Flask dashboard uses backend.live.market_stream directly and does not need
Kafka. Run this standalone bridge only if a Kafka broker is available:

    KAFKA_BROKERS=localhost:9092 python3 backend/kafka/binance_producer.py

No exchange API key or trading permission is used. The producer forwards raw
public trade messages; it does not make prediction or manipulation claims.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.live.market_stream import LiveMarketManager, configured_symbols  # noqa: E402

LOG = logging.getLogger("binance_kafka_bridge")


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    brokers = [value.strip() for value in os.getenv("KAFKA_BROKERS", "localhost:9092").split(",") if value.strip()]
    topic = os.getenv("KAFKA_TOPIC", "market_trades")
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
            request_timeout_ms=5_000,
            api_version_auto_timeout_ms=5_000,
            retries=3,
        )
        producer.partitions_for(topic)
    except Exception as exc:
        LOG.error("Kafka broker/topic unavailable (%s); bridge not started: %s", ",".join(brokers), exc)
        return 2

    manager = LiveMarketManager()
    count = 0
    def publish(trade: dict) -> None:
        nonlocal count
        producer.send(topic, trade)
        count += 1
        if count == 1 or count % 1000 == 0:
            LOG.info("Forwarded %d actual trades to topic %s", count, topic)

    manager.add_trade_listener(publish)
    result = manager.start(configured_symbols())
    if not result.get("ok"):
        producer.close(timeout=5)
        LOG.error("Could not start Binance stream: %s", result.get("error"))
        return 2
    LOG.info("Forwarding public Binance Spot trades for %s to Kafka topic %s", ", ".join(configured_symbols()), topic)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        LOG.info("Stopping bridge after %d trade messages", count)
    finally:
        manager.stop()
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
        except Exception:
            LOG.exception("Kafka producer cleanup failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
