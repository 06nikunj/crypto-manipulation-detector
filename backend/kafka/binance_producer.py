"""
Kafka Producer - Binance Live Trade Ingestion
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Nikunj
"""

import json
import time

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_MARKET = "market_trades"
TOPIC_TELEGRAM = "telegram_signals"

COINS = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "XRPUSDT", "SOLUSDT",
         "ADAUSDT", "SHIBUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT"]

def create_producer():
    """Initialize Kafka producer with JSON serializer."""
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_binance_trades(coin, limit=5):
    """Fetch latest trades from Binance REST API."""
    from binance.client import Client
    client = Client()
    trades = client.get_recent_trades(symbol=coin, limit=limit)
    return [
        {
            "coin": coin,
            "price": float(t["price"]),
            "volume": float(t["qty"]),
            "timestamp": t["time"],
            "is_buyer": t["isBuyerMaker"],
            "trade_id": t["id"]
        }
        for t in trades
    ]

def run_producer():
    """Main loop: fetch trades and push to Kafka every 2 seconds."""
    producer = create_producer()
    print("=" * 50)
    print("  KAFKA PRODUCER STARTED")
    print(f"  Broker: {KAFKA_BROKER}")
    print(f"  Topic:  {TOPIC_MARKET}")
    print(f"  Coins:  {len(COINS)} pairs")
    print("=" * 50)

    while True:
        for coin in COINS:
            try:
                trades = fetch_binance_trades(coin)
                for trade in trades:
                    producer.send(TOPIC_MARKET, value=trade)
                print(f"  ✅ {coin} → {len(trades)} trades sent")
            except Exception as e:
                print(f"  ❌ {coin} error: {e}")
        producer.flush()
        time.sleep(2)

if __name__ == "__main__":
    run_producer()
