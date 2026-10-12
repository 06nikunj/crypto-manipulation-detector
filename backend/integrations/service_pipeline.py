"""Kafka source/sink integration and a safe, bounded historical replay controller.

The direct Binance UI path remains usable without Kafka. When configured, the
same validated exchange trades are also published to Kafka for Spark.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from backend.integrations.influx_store import InfluxMetricsStore
from backend.integrations.neo4j_store import Neo4jGraphStore

LOG = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[2]


def _utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _normalize_utc_timestamp(value):
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError):
        return text


def _json_bytes(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


class KafkaPublisher:
    def __init__(self, brokers: str, market_topic="market_trades", output_topic="processed_events"):
        self.brokers = [part.strip() for part in brokers.split(",") if part.strip()]
        self.market_topic = market_topic
        self.output_topic = output_topic
        self._producer = None
        self._lock = threading.Lock()
        self.sent_live = 0
        self.sent_replay = 0
        self.errors = 0
        self.last_error = None
        self.last_sent_at = None

    @property
    def configured(self):
        return bool(self.brokers)

    def connect(self):
        if not self.configured:
            return {"status": "not_configured", "configured": False}
        with self._lock:
            existing = self._producer
        if existing is not None:
            try:
                if existing.partitions_for(self.market_topic):
                    return self.health()
            except Exception:
                pass
            with self._lock:
                if self._producer is existing:
                    self._producer = None
            try:
                existing.close(timeout=2)
            except Exception:
                pass
        producer = None
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=self.brokers,
                value_serializer=_json_bytes,
                key_serializer=lambda value: str(value).encode("utf-8") if value is not None else None,
                acks="all", retries=8, max_in_flight_requests_per_connection=1,
                linger_ms=max(0, int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "20"))),
                batch_size=max(1024, int(os.getenv("KAFKA_PRODUCER_BATCH_SIZE", "32768"))),
                request_timeout_ms=10_000,
            )
            # Metadata lookup proves the broker/topic is reachable before reporting ready.
            partitions = producer.partitions_for(self.market_topic)
            if not partitions:
                raise RuntimeError(f"Kafka topic {self.market_topic!r} is missing or has no partitions")
            with self._lock:
                self._producer = producer
                self.last_error = None
            return {"status": "connected", "configured": True,
                    "brokers": self.brokers, "topic": self.market_topic}
        except Exception as exc:
            if producer is not None:
                try:
                    producer.close(timeout=2)
                except Exception:
                    pass
            self.last_error = str(exc)[:240]
            return {"status": "unavailable", "configured": True, "error": self.last_error}

    def _send(self, topic, key, record, timeout=None):
        with self._lock:
            producer = self._producer
        if producer is None:
            raise RuntimeError(self.last_error or "Kafka producer is not connected")
        future = producer.send(topic, key=key, value=record)
        if timeout is not None:
            future.get(timeout=timeout)
            self.last_sent_at = _utc_now()
        return future

    def _live_publish_ack(self, _metadata):
        with self._lock:
            self.sent_live += 1
            self.last_sent_at = _utc_now()
            self.last_error = None

    def _publish_error(self, error):
        with self._lock:
            self.errors += 1
            self.last_error = str(error)[:240]
        LOG.warning("Kafka publish failed: %s", error)

    def publish_trade(self, trade: dict):
        if not self.configured:
            return
        trade_id = trade.get("trade_id")
        event_id = f"binance:{trade.get('symbol')}:{trade_id if trade_id is not None else uuid.uuid4().hex}"
        record = {
            "schema_version": 1, "record_type": "market_trade", "source": "BINANCE_SPOT",
            "event_id": event_id, "record_id": event_id,
            "event_timestamp": trade.get("exchange_trade_time"),
            "received_at": trade.get("received_at") or _utc_now(),
            "symbol": trade.get("symbol"), "price": trade.get("price"),
            "quantity": trade.get("quantity"), "volume": trade.get("quantity"),
            "quote_volume": trade.get("quote_quantity"),
            "trade_id": trade_id, "buyer_is_maker": bool(trade.get("buyer_is_maker")),
            "exchange": "Binance Spot public trade stream",
        }
        try:
            future = self._send(self.market_topic, event_id, record)
            future.add_callback(self._live_publish_ack)
            future.add_errback(self._publish_error)
        except Exception as exc:
            self._publish_error(exc)

    def publish_replay(self, record):
        record_id = str(record.get("record_id") or record.get("event_id") or uuid.uuid4().hex)
        self._send(self.market_topic, record_id, record, timeout=15)
        self.sent_replay += 1

    def health(self, probe=False):
        if not self.configured:
            return {"status": "not_configured", "configured": False,
                    "detail": "Set KAFKA_BOOTSTRAP_SERVERS to enable broker-backed processing."}
        with self._lock:
            producer = self._producer
        if producer is None:
            return {"status": "unavailable", "configured": True,
                    "error": self.last_error or "Kafka producer has not connected"}
        if probe:
            try:
                producer.partitions_for(self.market_topic)
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)[:240]
                with self._lock:
                    if self._producer is producer:
                        self._producer = None
                try:
                    producer.close(timeout=2)
                except Exception:
                    pass
                return {"status": "unavailable", "configured": True, "error": self.last_error}
        return {"status": "connected", "configured": True,
                "brokers": self.brokers, "topic": self.market_topic,
                "sent_live": self.sent_live, "sent_replay": self.sent_replay,
                "errors": self.errors, "last_sent_at": self.last_sent_at}

    def flush(self, timeout=10):
        with self._lock:
            producer = self._producer
        if producer:
            producer.flush(timeout=timeout)

    def close(self):
        with self._lock:
            producer, self._producer = self._producer, None
        if producer:
            try:
                producer.flush(timeout=10)
                producer.close(timeout=10)
            except Exception:
                LOG.exception("Kafka producer shutdown failed")


class KafkaCsvReplayController:
    """Thread-based replay with validated controls; never launches a shell process."""
    MAX_ROWS = 100_000

    def __init__(self, publisher: KafkaPublisher, csv_path=None):
        self.publisher = publisher
        self.csv_path = Path(csv_path or ROOT / "data/pump_dump_dataset/labeled_trades.csv")
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._paused = False
        self._thread = None
        self._state = {"status": "stopped", "published": 0, "total": 0,
                       "errors": 0, "replay_id": None, "source": "historical_benchmark"}

    def status(self):
        with self._lock:
            return dict(self._state)

    def start(self, speed=100.0, max_rows=1000):
        try:
            speed = float(speed)
            max_rows = int(max_rows)
            if not (0 < speed <= 100_000):
                raise ValueError("speed must be greater than 0 and at most 100000x")
            if not (1 <= max_rows <= self.MAX_ROWS):
                raise ValueError(f"max_rows must be between 1 and {self.MAX_ROWS}")
        except (TypeError, ValueError) as exc:
            return {"ok": False, "error": str(exc)}
        if not self.publisher.health().get("status") == "connected":
            return {"ok": False, "error": "Kafka is not connected; replay was not started."}
        with self._lock:
            if self._thread and self._thread.is_alive():
                return {"ok": False, "error": "A Kafka benchmark replay is already running.", "status": self.status()}
            if not self.csv_path.exists():
                return {"ok": False, "error": f"Benchmark CSV not found: {self.csv_path.name}"}
            self._stop.clear()
            self._paused = False
            self._state = {"status": "starting", "published": 0, "total": 0,
                           "errors": 0, "replay_id": uuid.uuid4().hex[:12],
                           "source": "historical_benchmark", "speed": speed,
                           "max_rows": max_rows}
            self._thread = threading.Thread(target=self._run, args=(speed, max_rows),
                                            daemon=True, name="KafkaBenchmarkReplay")
            self._thread.start()
        return {"ok": True, "message": "Kafka benchmark replay started", "status": self.status()}

    def _run(self, speed, max_rows):
        replay_id = self._state["replay_id"]
        try:
            with self.csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = []
                for index, row in enumerate(reader):
                    if index >= max_rows:
                        break
                    rows.append((index, row))
            with self._lock:
                self._state.update(status="running", total=len(rows))
            delay = min(1.0, 5.0 / speed)
            for index, row in rows:
                with self._condition:
                    while self._paused and not self._stop.is_set():
                        self._condition.wait(timeout=0.5)
                    if self._stop.is_set():
                        break
                try:
                    record = self.row_to_record(index, row, replay_id)
                    self.publisher.publish_replay(record)
                    with self._lock:
                        self._state["published"] += 1
                except Exception as exc:
                    with self._lock:
                        self._state["errors"] += 1
                        self._state["last_error"] = str(exc)[:240]
                    LOG.exception("Historical benchmark row %s could not be published", index)
                if delay and self._stop.wait(delay):
                    break
            self.publisher.flush(timeout=15)
        except Exception as exc:
            LOG.exception("Kafka historical replay failed")
            with self._lock:
                self._state["errors"] += 1
                self._state["last_error"] = str(exc)[:240]
        finally:
            with self._lock:
                self._state["status"] = "stopped" if self._stop.is_set() else "completed"
                self._state["finished_at"] = _utc_now()

    @staticmethod
    def row_to_record(index, row, replay_id):
        timestamp = _normalize_utc_timestamp(row.get("timestamp", ""))
        record_id = f"benchmark:{replay_id}:{index}"
        def number(name, default=0.0):
            try:
                value = float(row.get(name, default) or default)
                return value if value == value and abs(value) != float("inf") else default
            except (TypeError, ValueError):
                return default
        def integer(name, default=0):
            try:
                return int(float(row.get(name, default) or default))
            except (TypeError, ValueError):
                return default
        return {
            "schema_version": 1, "record_type": "historical_event",
            "source": "HISTORICAL_BENCHMARK", "replay_id": replay_id,
            "record_id": record_id, "event_id": str(row.get("event_id") or record_id),
            "event_timestamp": timestamp, "symbol": str(row.get("symbol", "")).upper(),
            "price": number("price"), "volume": number("volume"),
            "n_trades": integer("n_trades"), "rush_orders": integer("rush_orders"),
            "buy_ratio": number("buy_ratio", 0.5), "ground_truth": integer("is_pump"),
            "group": str(row.get("group") or ""), "exchange": str(row.get("exchange") or "unknown"),
            "ingested_at": _utc_now(),
        }

    def pause(self):
        with self._lock:
            if not self._thread or not self._thread.is_alive() or self._state["status"] not in {"running", "starting"}:
                return {"ok": False, "error": "No active Kafka benchmark replay."}
            self._paused = True
            self._state["status"] = "paused"
            return {"ok": True, "status": self.status()}

    def resume(self):
        with self._condition:
            if not self._thread or not self._thread.is_alive() or not self._paused:
                return {"ok": False, "error": "Replay is not paused."}
            self._paused = False
            self._state["status"] = "running"
            self._condition.notify_all()
            return {"ok": True, "status": self.status()}

    def stop(self):
        self._stop.set()
        with self._condition:
            self._paused = False
            self._condition.notify_all()
            thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=5)
        return {"ok": True, "status": self.status()}


class IntegrationPipeline:
    """Connects Binance/Kafka, Spark output consumption, Neo4j and InfluxDB."""
    def __init__(self):
        brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BROKERS", "")).strip()
        self.kafka = KafkaPublisher(
            brokers,
            os.getenv("KAFKA_MARKET_TOPIC", "market_trades"),
            os.getenv("KAFKA_OUTPUT_TOPIC", "processed_events"),
        )
        self.neo4j = Neo4jGraphStore()
        self.influx = InfluxMetricsStore()
        self.replay = KafkaCsvReplayController(self.kafka)
        self._stop = threading.Event()
        self._consumer_thread = None
        self._health_thread = None
        self._started = False
        self._lock = threading.RLock()
        self._recent_events = deque(maxlen=2000)
        self._recent_alerts = deque(maxlen=500)
        self.consumed_records = 0
        self.persistence_errors = 0
        self.last_consumer_at = None
        self.last_consumer_error = None
        self.last_health_metrics_at = None
        self._last_service_status = {}
        self._live_manager = None

    def start(self, live_manager=None):
        with self._lock:
            if self._started:
                return self.status()
            self._started = True
            self._live_manager = live_manager
        if self.kafka.configured:
            connector = threading.Thread(target=self._connect_loop, daemon=True,
                                         name="KafkaReconnectLoop")
            connector.start()
        self._health_thread = threading.Thread(target=self._health_loop, daemon=True,
                                               name="IntegrationHealthWriter")
        self._health_thread.start()
        return self.status()

    def _connect_loop(self):
        while not self._stop.is_set():
            result = self.kafka.connect()
            if result.get("status") == "connected":
                if self._live_manager is not None:
                    self._live_manager.add_trade_listener(self.kafka.publish_trade)
                if not (self._consumer_thread and self._consumer_thread.is_alive()):
                    self._consumer_thread = threading.Thread(target=self._consume_loop, daemon=True,
                                                             name="KafkaOutputConsumer")
                    self._consumer_thread.start()
                return
            LOG.warning("Kafka unavailable; retrying in 5 seconds: %s", result.get("error"))
            self._stop.wait(5)

    def _consume_loop(self):
        brokers = self.kafka.brokers
        topic = self.kafka.output_topic
        group_id = os.getenv("KAFKA_CONSUMER_GROUP", "cryptosentinel-api-v1")
        consumer = None
        try:
            from kafka import KafkaConsumer
            while not self._stop.is_set():
                try:
                    if consumer is None:
                        consumer = KafkaConsumer(
                            topic, bootstrap_servers=brokers, group_id=group_id,
                            enable_auto_commit=False, auto_offset_reset="earliest",
                            consumer_timeout_ms=1000,
                            max_poll_records=max(1, min(int(os.getenv("KAFKA_CONSUMER_MAX_RECORDS", "200")), 2000)),
                            request_timeout_ms=10_000, session_timeout_ms=10_000,
                        )
                    records_by_partition = consumer.poll(timeout_ms=1000, max_records=200)
                    messages = [msg for group in records_by_partition.values() for msg in group]
                    if not messages:
                        continue
                    payloads = []
                    valid_messages = []
                    for msg in messages:
                        try:
                            value = json.loads(msg.value.decode("utf-8"))
                            if not isinstance(value, dict):
                                raise ValueError("Kafka value must be a JSON object")
                            payloads.append(value)
                            valid_messages.append(msg)
                        except Exception as exc:
                            self.last_consumer_error = f"Invalid Kafka output: {str(exc)[:180]}"
                            self.persistence_errors += 1
                            payloads.append({"record_type": "batch_metrics", "source": "API_CONSUMER",
                                             "timestamp": _utc_now(), "input_records": 1,
                                             "processed_events": 0, "flagged_predictions": 0,
                                             "normal_predictions": 0, "invalid_records": 1,
                                             "processing_errors": 1, "processing_latency_ms": 0.0})
                    if payloads:
                        self._handle_output_records(payloads)
                    consumer.commit()
                    self.consumed_records += len(messages)
                    self.last_consumer_at = _utc_now()
                    self.last_consumer_error = None
                except Exception as exc:
                    self.persistence_errors += 1
                    self.last_consumer_error = str(exc)[:240]
                    LOG.exception("Kafka output consumer/persistence error")
                    if consumer is not None:
                        try:
                            consumer.close(autocommit=False)
                        except Exception:
                            pass
                        consumer = None
                    self._stop.wait(2)
        except Exception as exc:
            self.last_consumer_error = str(exc)[:240]
            LOG.exception("Kafka output consumer could not start")
        finally:
            if consumer is not None:
                try:
                    consumer.close(autocommit=False)
                except Exception:
                    pass

    def _handle_output_records(self, records):
        events = [item for item in records if item.get("record_type") == "processed_event"]
        metrics = [item for item in records if item.get("record_type") == "batch_metrics"]
        if self.neo4j.configured:
            if events:
                self.neo4j.write_events(events)
        if self.influx.configured:
            for item in metrics:
                self.influx.write_batch_metrics(item)
        if events:
            with self._lock:
                for item in events:
                    self._recent_events.append(dict(item))
                    if item.get("label") == "PUMP_AND_DUMP" or item.get("is_alert"):
                        self._recent_alerts.append(dict(item))

    def recent_events(self, limit=200):
        with self._lock:
            return list(reversed(list(self._recent_events)[-max(1, min(int(limit), 2000)):]))

    def recent_alerts(self, limit=100):
        with self._lock:
            return list(reversed(list(self._recent_alerts)[-max(1, min(int(limit), 500)):]))

    def _health_loop(self):
        while not self._stop.is_set():
            try:
                statuses = self.service_status(probe_kafka=True, probe_spark=True)
                self._last_service_status = statuses
                if self.kafka.configured and statuses["kafka"].get("status") != "connected":
                    self.kafka.connect()
                if self.influx.configured and self.influx.health().get("status") == "connected":
                    self.influx.write_service_health(statuses)
                    self.last_health_metrics_at = _utc_now()
            except Exception:
                LOG.debug("Service health metric write skipped", exc_info=True)
            if self._stop.wait(30):
                break

    def service_status(self, probe_kafka=False, probe_spark=False):
        kafka = self.kafka.health(probe=probe_kafka)
        spark = spark_health()
        neo4j = self.neo4j.health()
        influx = self.influx.health()
        grafana = grafana_health()
        return {"kafka": kafka, "spark": spark, "neo4j": neo4j,
                "influxdb": influx, "grafana": grafana}

    def current_service_status(self):
        with self._lock:
            cached = dict(self._last_service_status)
        return cached if cached else self.service_status(probe_kafka=True, probe_spark=True)

    def status(self):
        with self._lock:
            return {"status": "running" if self._started else "stopped",
                    "kafka": self.kafka.health(), "replay": self.replay.status(),
                    "consumed_records": self.consumed_records,
                    "persistence_errors": self.persistence_errors,
                    "last_consumer_at": self.last_consumer_at,
                    "last_consumer_error": self.last_consumer_error,
                    "last_health_metrics_at": self.last_health_metrics_at,
                    "services": dict(self._last_service_status),
                    "recent_processed_events": len(self._recent_events),
                    "recent_alerts": len(self._recent_alerts)}

    def metrics(self, limit=500, time_range="1h"):
        from backend.integrations.influx_store import _ALLOWED_RANGE
        if time_range not in _ALLOWED_RANGE:
            raise ValueError(f"time_range must be one of {', '.join(sorted(_ALLOWED_RANGE))}")
        if not self.influx.configured:
            return {"available": False, "source": "influxdb",
                    "error": "InfluxDB is not configured; no metrics are substituted."}
        health = self.influx.health()
        if health.get("status") != "connected":
            return {"available": False, "source": "influxdb", "error": health.get("error", health.get("detail"))}
        return self.influx.query_recent(limit=limit, time_range=time_range)

    def stop(self):
        self._stop.set()
        self.replay.stop()
        if self._live_manager is not None:
            self._live_manager.remove_trade_listener(self.kafka.publish_trade)
            self._live_manager = None
        for thread in (self._consumer_thread, self._health_thread):
            if thread and thread is not threading.current_thread():
                thread.join(timeout=4)
        self.kafka.close()
        self.neo4j.close()
        self.influx.close()
        self._started = False


def spark_health():
    import urllib.request
    url = os.getenv("SPARK_HEALTH_URL", "").strip()
    if not url:
        return {"status": "not_configured", "configured": False,
                "detail": "Set SPARK_HEALTH_URL when running the Structured Streaming processor."}
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            data = json.loads(response.read(65_536).decode("utf-8"))
            data.setdefault("configured", True)
            return data
    except Exception as exc:
        return {"status": "unavailable", "configured": True, "error": str(exc)[:200]}


def grafana_health():
    import urllib.request
    url = os.getenv("GRAFANA_HEALTH_URL", "").strip()
    if not url:
        return {"status": "not_configured", "configured": False,
                "detail": "Set GRAFANA_HEALTH_URL to measure Grafana availability."}
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            data = json.loads(response.read(16_384).decode("utf-8"))
        if str(data.get("status", "")).lower() != "ok":
            raise RuntimeError(f"Grafana health status: {data.get('status', 'unknown')}")
        return {"status": "healthy", "configured": True,
                "database": data.get("database", "unknown")}
    except Exception as exc:
        return {"status": "unavailable", "configured": True, "error": str(exc)[:200]}


pipeline = IntegrationPipeline()
