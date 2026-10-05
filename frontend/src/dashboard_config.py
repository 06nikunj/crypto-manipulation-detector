"""
Grafana Dashboard Configuration - Frontend Module
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Aanya
"""

import json

# Grafana dashboard configuration for live alerts
dashboard_config = {
    "title": "Crypto Manipulation Detection - Live Dashboard",
    "panels": [
        {
            "id": 1,
            "title": "Live Price Feed",
            "type": "timeseries",
            "datasource": "InfluxDB",
            "coins": ["BTC/USDT", "ETH/USDT", "DOGE/USDT", "XRP/USDT", "SOL/USDT"]
        },
        {
            "id": 2,
            "title": "Manipulation Alerts",
            "type": "alert-list",
            "categories": ["PUMP_AND_DUMP", "WASH_TRADE", "SPOOFING"]
        },
        {
            "id": 3,
            "title": "Volume Spike Monitor",
            "type": "gauge",
            "threshold": {"warning": 5, "critical": 15}
        },
        {
            "id": 4,
            "title": "Transaction Graph View",
            "type": "node-graph",
            "datasource": "Neo4j"
        }
    ],
    "refresh": "5s",
    "timezone": "IST"
}

if __name__ == "__main__":
    print("=" * 50)
    print("  GRAFANA DASHBOARD CONFIG GENERATED")
    print("=" * 50)
    print(json.dumps(dashboard_config, indent=2))
    print("\nPanels configured:", len(dashboard_config["panels"]))
    print("Auto-refresh:", dashboard_config["refresh"])
