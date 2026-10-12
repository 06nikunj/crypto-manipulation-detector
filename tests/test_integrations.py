import csv
import math
import os
import unittest
from pathlib import Path
from collections import deque

ROOT = Path(__file__).resolve().parents[1]


class KafkaRecordTests(unittest.TestCase):
    def test_replay_row_preserves_benchmark_provenance_and_stable_key(self):
        from backend.integrations.service_pipeline import KafkaCsvReplayController
        row = {
            "timestamp": "2024-01-01T00:00:00Z", "event_id": "observed-1",
            "symbol": "BTCUSDT", "price": "42000.5", "volume": "1.25",
            "n_trades": "9", "rush_orders": "2", "buy_ratio": "0.6",
            "is_pump": "1", "group": "pump-window-7", "exchange": "binance",
        }
        first = KafkaCsvReplayController.row_to_record(3, row, "run-abc")
        second = KafkaCsvReplayController.row_to_record(3, row, "run-abc")
        self.assertEqual(first["record_type"], "historical_event")
        self.assertEqual(first["source"], "HISTORICAL_BENCHMARK")
        self.assertEqual(first["record_id"], second["record_id"])
        self.assertEqual(first["event_id"], "observed-1")
        self.assertEqual(first["ground_truth"], 1)
        self.assertEqual(first["price"], 42000.5)
        self.assertEqual(first["n_trades"], 9)

    def test_nonfinite_source_values_are_safely_defaulted(self):
        from backend.integrations.service_pipeline import KafkaCsvReplayController
        row = {"timestamp": "2024-01-01T00:00:00Z", "symbol": "BAD", "price": "nan",
               "volume": "inf", "buy_ratio": "-inf", "is_pump": "invalid"}
        record = KafkaCsvReplayController.row_to_record(0, row, "run")
        self.assertEqual(record["price"], 0.0)
        self.assertEqual(record["volume"], 0.0)
        self.assertEqual(record["buy_ratio"], 0.5)
        self.assertEqual(record["ground_truth"], 0)
        for value in (record["price"], record["volume"], record["buy_ratio"]):
            self.assertTrue(math.isfinite(value))

    def test_unconfigured_broker_is_explicit_and_does_not_claim_connected(self):
        from backend.integrations.service_pipeline import KafkaPublisher
        publisher = KafkaPublisher("")
        self.assertEqual(publisher.connect()["status"], "not_configured")
        self.assertEqual(publisher.health()["status"], "not_configured")
        publisher.publish_trade({"symbol": "BTCUSDT", "trade_id": 1, "price": 1.0, "quantity": 1.0})
        self.assertEqual(publisher.sent_live, 0)
        self.assertEqual(publisher.errors, 0)

    def test_actual_trade_record_is_acknowledged_and_attributed(self):
        from backend.integrations.service_pipeline import KafkaPublisher
        class Future:
            def add_callback(self, callback):
                callback(object())
                return self
            def add_errback(self, _callback):
                return self
        class Producer:
            def __init__(self): self.calls = []
            def send(self, topic, **kwargs):
                self.calls.append((topic, kwargs))
                return Future()
        publisher = KafkaPublisher("broker:9092")
        fake = Producer()
        publisher._producer = fake
        publisher.publish_trade({"symbol": "BTCUSDT", "trade_id": 42,
                                 "exchange_trade_time": "2024-01-01T00:00:00Z",
                                 "received_at": "2024-01-01T00:00:00.100Z",
                                 "price": 42000.0, "quantity": 0.02,
                                 "quote_quantity": 840.0, "buyer_is_maker": False})
        self.assertEqual(publisher.sent_live, 1)
        topic, message = fake.calls[0]
        self.assertEqual(topic, "market_trades")
        self.assertEqual(message["value"]["source"], "BINANCE_SPOT")
        self.assertEqual(message["value"]["record_type"], "market_trade")
        self.assertEqual(message["value"]["event_id"], "binance:BTCUSDT:42")
        self.assertEqual(message["value"]["event_timestamp"], "2024-01-01T00:00:00Z")


class BenchmarkInferenceTests(unittest.TestCase):
    def test_worker_loads_saved_xgboost_and_scaler_and_scores_a_real_csv_row(self):
        from backend.streaming.spark_processor import _load_model, score_benchmark_event
        from backend.integrations.service_pipeline import KafkaCsvReplayController
        csv_path = ROOT / "data/pump_dump_dataset/labeled_trades.csv"
        self.assertTrue(csv_path.is_file(), "bundled labeled benchmark CSV is required")
        with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
            source_row = next(csv.DictReader(handle))
        event = KafkaCsvReplayController.row_to_record(0, source_row, "test-replay")
        model_bundle = _load_model()
        model, scaler, info = model_bundle
        self.assertIsNotNone(model, info.get("reason"))
        self.assertIsNotNone(scaler, "inference must use the training scaler")
        result = score_benchmark_event(event, [], model_bundle)
        self.assertEqual(result["detection_method"], "xgboost_benchmark_model")
        self.assertEqual(result["source"], "HISTORICAL_BENCHMARK")
        self.assertIn(result["model_prediction"], (0, 1))
        self.assertGreaterEqual(result["risk_score"], 0.0)
        self.assertLessEqual(result["risk_score"], 1.0)
        self.assertEqual(result["model_threshold"], 0.50)
        self.assertEqual(result["model_available"], True)

    def test_live_bar_warns_only_on_explicit_rules_and_never_claims_xgboost_prediction(self):
        from backend.streaming.spark_processor import build_live_bar_event
        state = {"previous_close": 100.0, "volumes": deque([1.0] * 20, maxlen=20)}
        result = build_live_bar_event({"symbol": "BTCUSDT", "window_start": "2024-01-01T00:00:05Z",
                                       "open": 100.0, "high": 102.0, "low": 100.0, "close": 102.0,
                                       "base_volume": 3.0, "quote_volume": 306.0,
                                       "trade_count": 6, "taker_buy_base_volume": 2.0}, state)
        self.assertEqual(result["source"], "BINANCE_SPOT")
        self.assertEqual(result["label"], "RULE_WARNING")
        self.assertEqual(set(result["signals"]), {"rapid_price_change", "volume_spike"})
        self.assertIsNone(result["model_prediction"])
        self.assertIsNone(result["ground_truth"])
        self.assertFalse(result["model_available"])
        self.assertIn("not evidence of manipulation", result["explanation"])

    def test_feature_contract_matches_saved_model_shape(self):
        import joblib
        from backend.spark.src.preprocessing import FEATURE_COLS
        model = joblib.load(ROOT / "models/xgboost_detector.joblib")
        scaler = joblib.load(ROOT / "models/scaler.joblib")
        self.assertEqual(len(FEATURE_COLS), 12)
        self.assertEqual(model.n_features_in_, len(FEATURE_COLS))
        self.assertEqual(scaler.n_features_in_, len(FEATURE_COLS))


class StoreAndApiTests(unittest.TestCase):
    def test_unconfigured_store_status_is_honest(self):
        from backend.integrations.neo4j_store import Neo4jGraphStore
        from backend.integrations.influx_store import InfluxMetricsStore
        neo = Neo4jGraphStore(uri="", password="")
        influx = InfluxMetricsStore(url="", token="", org="", bucket="")
        self.assertEqual(neo.health()["status"], "not_configured")
        self.assertEqual(influx.health()["status"], "not_configured")
        self.assertEqual(neo.write_events([]), 0)

    def test_api_consumer_routes_processed_events_and_metrics_to_real_store_adapters(self):
        from backend.integrations.service_pipeline import IntegrationPipeline
        class Neo4jSink:
            configured = True
            def __init__(self): self.events = []
            def write_events(self, events): self.events.extend(events); return len(events)
        class InfluxSink:
            configured = True
            def __init__(self): self.metrics = []
            def write_batch_metrics(self, item): self.metrics.append(item); return True
        pipeline = IntegrationPipeline()
        pipeline.neo4j = Neo4jSink()
        pipeline.influx = InfluxSink()
        event = {"record_type": "processed_event", "record_id": "event-1",
                 "symbol": "BTCUSDT", "label": "PUMP_AND_DUMP", "is_alert": True}
        metric = {"record_type": "batch_metrics", "processed_events": 1}
        pipeline._handle_output_records([event, metric])
        self.assertEqual(pipeline.neo4j.events, [event])
        self.assertEqual(pipeline.influx.metrics, [metric])
        self.assertEqual(pipeline.recent_events(1), [event])
        self.assertEqual(pipeline.recent_alerts(1), [event])
        pipeline.replay.stop()

    def test_influx_query_rejects_unsafe_or_unsupported_time_ranges(self):
        from backend.integrations.influx_store import InfluxMetricsStore
        store = InfluxMetricsStore(url="http://127.0.0.1:8086", token="x", org="x", bucket="x")
        with self.assertRaises(ValueError):
            store.query_recent(time_range="1h |> drop()")

    def test_pipeline_status_and_metrics_api_reflect_missing_services(self):
        # Ensure the app reports optional dependencies as unconfigured, rather than faking health.
        for name in ("KAFKA_BOOTSTRAP_SERVERS", "NEO4J_URI", "NEO4J_PASSWORD",
                     "INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG", "INFLUXDB_BUCKET",
                     "SPARK_HEALTH_URL", "GRAFANA_HEALTH_URL"):
            os.environ.pop(name, None)
        from backend.api.app import app
        app.config.update(TESTING=True)
        client = app.test_client()
        status = client.get("/api/pipeline/status")
        self.assertEqual(status.status_code, 200)
        data = status.get_json()
        self.assertEqual(data["application"], "healthy")
        self.assertEqual(data["services"]["kafka"]["status"], "not_configured")
        self.assertEqual(data["services"]["neo4j"]["status"], "not_configured")
        self.assertEqual(data["services"]["influxdb"]["status"], "not_configured")
        metrics = client.get("/api/metrics?range=1h")
        self.assertEqual(metrics.status_code, 200)
        self.assertFalse(metrics.get_json()["available"])
        bad_metrics = client.get("/api/metrics?range=1h%20%7C%3E%20drop()")
        self.assertEqual(bad_metrics.status_code, 400)


if __name__ == "__main__":
    unittest.main()
