"""Spark Structured Streaming worker for Kafka market and benchmark events.

Live Binance trades are event-time aggregated into non-empty five-second bars
and evaluated with transparent rules only. Historical benchmark rows use the
project's existing 12-feature extractor, saved scaler, and XGBoost artifact.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

LOG = logging.getLogger("cryptosentinel.spark")
ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 1
ALERT_THRESHOLD = 0.50
MAX_BATCH_ROWS = 10_000

_STATUS = {"status": "starting", "service": "spark-structured-streaming",
           "started_at": None, "run_id": uuid.uuid4().hex[:12],
           "queries": {}, "counts": {"input_records": 0, "processed_events": 0,
           "flagged_predictions": 0, "invalid_records": 0, "processing_errors": 0},
           "last_batch_at": None, "last_error": None, "model": {"loaded": False}}
_STATUS_LOCK = threading.RLock()
_HISTORIES = defaultdict(lambda: deque(maxlen=10))
_REPLAY_IDS = {}
_BAR_STATE = defaultdict(lambda: {"previous_close": None, "volumes": deque(maxlen=20)})


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json_safe(value):
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _health_payload():
    with _STATUS_LOCK:
        return json.loads(json.dumps(_STATUS, default=str))


def _health_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if urlparse(self.path).path not in ("/health", "/ready"):
                self.send_error(404)
                return
            payload = _health_payload()
            payload["configured"] = True
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            self.send_response(200 if payload.get("status") in ("healthy", "processing", "starting") else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    port = int(os.getenv("SPARK_HEALTH_PORT", "8090"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True, name="SparkHealthHTTP").start()
    return server


def _input_schema():
    from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType,
                                   StringType, StructField, StructType)
    return StructType([
        StructField("schema_version", IntegerType(), True),
        StructField("record_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("record_id", StringType(), True),
        StructField("replay_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("received_at", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("quote_volume", DoubleType(), True),
        StructField("trade_id", StringType(), True),
        StructField("buyer_is_maker", BooleanType(), True),
        StructField("n_trades", LongType(), True),
        StructField("rush_orders", LongType(), True),
        StructField("buy_ratio", DoubleType(), True),
        StructField("ground_truth", IntegerType(), True),
        StructField("group", StringType(), True),
        StructField("exchange", StringType(), True),
    ])


def _parsed_input(spark, market_topic, brokers, max_offsets):
    from pyspark.sql import functions as F
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", brokers)
           .option("subscribe", market_topic)
           .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
           .option("maxOffsetsPerTrigger", str(max_offsets))
           .option("failOnDataLoss", "false")
           .load())
    parsed = raw.selectExpr("CAST(value AS STRING) AS raw_json", "timestamp AS kafka_timestamp") \
        .withColumn("payload", F.from_json(F.col("raw_json"), _input_schema()))
    columns = [F.col(f"payload.{name}").alias(name) for name in (
        "schema_version", "record_type", "source", "event_id", "record_id", "replay_id",
        "event_timestamp", "received_at", "symbol", "price", "quantity", "volume",
        "quote_volume", "trade_id", "buyer_is_maker", "n_trades", "rush_orders",
        "buy_ratio", "ground_truth", "group", "exchange")]
    parsed = parsed.select("raw_json", "kafka_timestamp", *columns)
    parsed = parsed.withColumn("symbol", F.upper(F.trim(F.col("symbol")))) \
        .withColumn("event_time", F.to_timestamp("event_timestamp")) \
        .withColumn("volume", F.coalesce(F.col("volume"), F.col("quantity")))
    valid = (F.col("record_type").isin("historical_event", "market_trade") &
             F.col("source").isin("HISTORICAL_BENCHMARK", "BINANCE_SPOT") &
             F.col("record_id").isNotNull() & F.col("symbol").rlike("^[A-Z0-9]{4,20}$") &
             F.col("event_time").isNotNull() & F.col("price").isNotNull() & (F.col("price") > 0) &
             F.col("volume").isNotNull() & (F.col("volume") >= 0))
    return parsed.withColumn("_valid", F.coalesce(valid, F.lit(False)))


def _write_kafka(spark, records, topic, brokers):
    if not records:
        return
    from pyspark.sql.types import StringType, StructField, StructType
    rows = []
    for item in records:
        safe = _json_safe(item)
        key = str(safe.get("record_id") or safe.get("event_id") or uuid.uuid4().hex)
        rows.append((key, json.dumps(safe, separators=(",", ":"), allow_nan=False)))
    schema = StructType([StructField("key", StringType(), False), StructField("value", StringType(), False)])
    (spark.createDataFrame(rows, schema=schema)
     .selectExpr("CAST(key AS BINARY) AS key", "CAST(value AS BINARY) AS value")
     .write.format("kafka")
     .option("kafka.bootstrap.servers", brokers)
     .option("topic", topic)
     .save())


def _metric_record(source, batch_id, counts, latency_ms):
    return {
        "schema_version": SCHEMA_VERSION, "record_type": "batch_metrics",
        "source": source, "run_id": _STATUS["run_id"], "batch_id": str(batch_id),
        "timestamp": _now(), "input_records": int(counts.get("input_records", 0)),
        "processed_events": int(counts.get("processed_events", 0)),
        "flagged_predictions": int(counts.get("flagged_predictions", 0)),
        "normal_predictions": int(counts.get("normal_predictions", 0)),
        "invalid_records": int(counts.get("invalid_records", 0)),
        "processing_errors": int(counts.get("processing_errors", 0)),
        "processing_latency_ms": round(float(latency_ms), 3),
        "dropped_records": int(counts.get("dropped_records", 0)),
    }


def _update_counts(metrics):
    with _STATUS_LOCK:
        for key in ("input_records", "processed_events", "flagged_predictions", "invalid_records", "processing_errors"):
            _STATUS["counts"][key] = int(_STATUS["counts"].get(key, 0)) + int(metrics.get(key, 0))
        _STATUS["last_batch_at"] = metrics.get("timestamp")


def _write_output(spark, records, metrics, brokers, output_topic):
    payloads = list(records)
    if metrics:
        payloads.append(metrics)
    _write_kafka(spark, payloads, output_topic, brokers)
    if metrics:
        _update_counts(metrics)


def score_benchmark_event(base, history, model_bundle):
    """Apply the project's existing feature extractor and saved scaler/model to one benchmark row."""
    from backend.spark.src.preprocessing import FEATURE_COLS, extract_single_event_features
    model, scaler, model_info = model_bundle
    features = extract_single_event_features(base, list(history))
    vector = [float(features.get(name, 0.0)) for name in FEATURE_COLS]
    if not all(value == value and abs(value) != float("inf") for value in vector):
        raise ValueError("feature vector contains non-finite values")
    if model is not None:
        import numpy as np
        arr = np.asarray([vector], dtype=float)
        arr = scaler.transform(arr)
        probabilities = model.predict_proba(arr)[0]
        classes = list(getattr(model, "classes_", [0, 1]))
        positive_index = classes.index(1) if 1 in classes else len(probabilities) - 1
        score = float(probabilities[positive_index])
        detection_method = "xgboost_benchmark_model"
    else:
        rush = float(features.get("rush_order_ratio", 0))
        anomaly = float(features.get("anomaly_score", 0))
        vol_spike = float(features.get("volume_change_pct", 0))
        price_spike = float(features.get("price_change_pct", 0))
        score = max(0.0, min(1.0, rush * .40 + min(anomaly / 5, 1) * .25 + min(vol_spike / 10, 1) * .20 + min(abs(price_spike) / .10, 1) * .15))
        detection_method = "historical_rule_fallback"
    predicted = int(score >= ALERT_THRESHOLD)
    explanation = "" if model is not None else "Fallback rule score; saved XGBoost model/scaler unavailable."
    if model is not None:
        importances = list(getattr(model, "feature_importances_", []))
        if len(importances) == len(FEATURE_COLS):
            top = sorted(zip(FEATURE_COLS, vector, importances), key=lambda x: x[2], reverse=True)[:4]
            explanation = "; ".join(f"{name}={value:.5g} (importance {importance:.4f})" for name, value, importance in top)
    alert_id = f"alert:{base['record_id']}" if predicted else None
    return {**base, **features, "record_type": "processed_event",
            "model_prediction": predicted, "predicted_class": predicted,
            "risk_score": round(score, 6),
            "label": "PUMP_AND_DUMP" if predicted else "NORMAL",
            "alert_id": alert_id, "is_alert": bool(predicted),
            "detection_method": detection_method, "model_threshold": ALERT_THRESHOLD,
            "model_available": bool(model is not None),
            "model_info": model_info, "explanation": explanation}


def _process_history(spark, batch_df, batch_id, brokers, output_topic, model_bundle):
    started = time.monotonic()
    records, errors = [], 0
    rows = batch_df.orderBy("event_time", "symbol", "record_id").limit(MAX_BATCH_ROWS).collect()
    model, scaler, model_info = model_bundle
    for row in rows:
        event = row.asDict(recursive=True)
        replay_id = event.get("replay_id")
        symbol = event["symbol"]
        if replay_id and _REPLAY_IDS.get(symbol) != replay_id:
            _HISTORIES[symbol].clear()
            _REPLAY_IDS[symbol] = replay_id
        base = {"record_id": event["record_id"], "event_id": event.get("event_id") or event["record_id"],
                "event_timestamp": event["event_timestamp"], "symbol": symbol,
                "price": float(event["price"]), "volume": float(event["volume"]),
                "n_trades": int(event.get("n_trades") or 0), "rush_orders": int(event.get("rush_orders") or 0),
                "buy_ratio": float(event.get("buy_ratio") if event.get("buy_ratio") is not None else 0.5),
                "ground_truth": event.get("ground_truth"), "group": event.get("group"),
                "exchange": event.get("exchange"), "source": "HISTORICAL_BENCHMARK",
                "replay_id": replay_id}
        try:
            records.append(score_benchmark_event(base, _HISTORIES[symbol], model_bundle))
            _HISTORIES[symbol].append(base)
        except Exception as exc:
            errors += 1
            records.append({**base, "record_type": "processed_event", "label": "ERROR",
                            "model_prediction": None, "predicted_class": None, "risk_score": None,
                            "is_alert": False, "detection_method": "processing_error",
                            "model_available": bool(model is not None), "explanation": str(exc)[:240]})
    flagged = sum(item.get("model_prediction") == 1 for item in records)
    metrics = _metric_record("HISTORICAL_BENCHMARK", batch_id,
                             {"input_records": len(rows), "processed_events": len(records),
                              "flagged_predictions": flagged,
                              "normal_predictions": sum(item.get("model_prediction") == 0 for item in records),
                              "processing_errors": errors,
                              "dropped_records": max(0, batch_df.count() - len(rows))},
                             (time.monotonic() - started) * 1000)
    _write_output(spark, records, metrics, brokers, output_topic)


def build_live_bar_event(item, state):
    """Turn one actual, closed non-empty market bar into a transparent rule event."""
    symbol = str(item["symbol"])
    close = float(item["close"])
    volume = float(item["base_volume"] or 0.0)
    previous = state["previous_close"]
    price_move = (close - previous) / previous if previous and previous > 0 else None
    baseline = list(state["volumes"])
    volume_ratio = None
    if len(baseline) >= 20:
        import statistics
        median = statistics.median(baseline)
        volume_ratio = volume / median if median > 0 else None
    signals = []
    if price_move is not None and abs(price_move) >= 0.01:
        signals.append("rapid_price_change")
    if volume_ratio is not None and volume_ratio >= 3.0:
        signals.append("volume_spike")
    start = _json_safe(item.get("window_start"))
    event_id = f"binance-bar:{symbol}:{start}"
    record = {
            "schema_version": SCHEMA_VERSION, "record_type": "processed_event",
            "source": "BINANCE_SPOT", "record_id": event_id, "event_id": event_id,
            "event_timestamp": start, "symbol": symbol, "price": close,
            "open": item.get("open"), "high": item.get("high"), "low": item.get("low"),
            "volume": volume, "quote_volume": float(item.get("quote_volume") or 0),
            "n_trades": int(item.get("trade_count") or 0),
            "taker_buy_base_volume": float(item.get("taker_buy_base_volume") or 0),
            "price_change_pct": price_move, "volume_ratio_vs_median": volume_ratio,
            "ground_truth": None, "model_prediction": None, "predicted_class": None,
            "risk_score": None, "model_available": False,
            "model_threshold": ALERT_THRESHOLD,
            "label": "RULE_WARNING" if signals else "OBSERVED",
            "is_alert": bool(signals), "signals": signals,
            "detection_method": "spark_live_five_second_market_rules",
            "explanation": ("Observed rule threshold crossed; this is a review cue, not evidence of manipulation."
                            if signals else "Observed Binance Spot market bar; no displayed heuristic threshold crossed."),
            "data_limitations": "No live ground-truth labels; benchmark XGBoost score is not applied to this live feed.",
        }
    state["previous_close"] = close
    state["volumes"].append(volume)
    return record


def _process_live_bars(spark, batch_df, batch_id, brokers, output_topic):
    started = time.monotonic()
    rows = batch_df.orderBy("symbol", "window_start").limit(MAX_BATCH_ROWS).collect()
    records = []
    for row in rows:
        item = row.asDict(recursive=True)
        records.append(build_live_bar_event(item, _BAR_STATE[str(item["symbol"])]))
    input_records = sum(int(row["trade_count"] or 0) for row in rows)
    metrics = _metric_record("BINANCE_SPOT", batch_id,
                             {"input_records": input_records, "processed_events": len(records),
                              "flagged_predictions": sum(bool(row["is_alert"]) for row in records),
                              "normal_predictions": 0},
                             (time.monotonic() - started) * 1000)
    _write_output(spark, records, metrics, brokers, output_topic)


def _process_invalid(spark, batch_df, batch_id, brokers, dead_letter_topic, output_topic):
    started = time.monotonic()
    rows = batch_df.limit(MAX_BATCH_ROWS).collect()
    bad = []
    for row in rows:
        value = row.asDict(recursive=True)
        reason = "malformed JSON" if value.get("source") is None else "missing or invalid required field / unsupported source"
        bad.append({"schema_version": SCHEMA_VERSION, "record_type": "invalid_event",
                    "source": value.get("source") or "unknown", "record_id": value.get("record_id") or uuid.uuid4().hex,
                    "received_at": _now(), "reason": reason,
                    "raw_payload": str(value.get("raw_json", ""))[:2000]})
    _write_kafka(spark, bad, dead_letter_topic, brokers)
    metrics = _metric_record("INVALID_INPUT", batch_id,
                             {"input_records": len(rows), "processed_events": 0,
                              "invalid_records": len(bad)}, (time.monotonic() - started) * 1000)
    _write_output(spark, [], metrics, brokers, output_topic)


def _load_model():
    import joblib
    import numpy as np
    from backend.spark.src.preprocessing import FEATURE_COLS
    model_path = Path(os.getenv("MODEL_PATH", str(ROOT / "models/xgboost_detector.joblib")))
    scaler_path = Path(os.getenv("SCALER_PATH", str(ROOT / "models/scaler.joblib")))
    info = {"loaded": False, "model_path": str(model_path.name), "scaler_path": str(scaler_path.name),
            "feature_count": len(FEATURE_COLS), "threshold": ALERT_THRESHOLD,
            "category": "binary pump-window versus normal benchmark classification"}
    if not model_path.exists():
        info["reason"] = "Saved XGBoost model artifact is not present; benchmark-only rule fallback will be used."
        return None, None, info
    if not scaler_path.exists():
        info["reason"] = "Saved feature scaler is not present; benchmark-only rule fallback will be used."
        return None, None, info
    try:
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path)
        if scaler is not None and int(getattr(scaler, "n_features_in_", -1)) != len(FEATURE_COLS):
            raise ValueError("saved scaler feature count does not match the 12-column benchmark contract")
        if int(getattr(model, "n_features_in_", len(FEATURE_COLS))) != len(FEATURE_COLS):
            raise ValueError("saved XGBoost feature count does not match the 12-column benchmark contract")
        probe = np.zeros((1, len(FEATURE_COLS)), dtype=float)
        transformed = scaler.transform(probe) if scaler is not None else probe
        probabilities = model.predict_proba(transformed)
        if probabilities.shape[1] < 2:
            raise ValueError("saved classifier does not expose binary class probabilities")
        info.update(loaded=True, scaler_loaded=scaler is not None,
                    classes=[int(item) for item in getattr(model, "classes_", [0, 1])])
        return model, scaler, info
    except Exception as exc:
        info["reason"] = f"Model/scaler validation failed: {str(exc)[:200]}"
        LOG.exception("Saved benchmark XGBoost artifacts could not be loaded")
        return None, None, info


def run():
    from pyspark.sql import SparkSession, functions as F
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    market_topic = os.getenv("KAFKA_MARKET_TOPIC", "market_trades")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_events")
    dead_letter_topic = os.getenv("KAFKA_DEAD_LETTER_TOPIC", "invalid_events")
    max_offsets = max(1, min(int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "500")), 5000))
    checkpoint = Path(os.getenv("SPARK_CHECKPOINT_DIR", "/var/lib/cryptosentinel/checkpoints"))
    checkpoint.mkdir(parents=True, exist_ok=True)
    health = _health_server()
    with _STATUS_LOCK:
        _STATUS.update(started_at=_now(), status="starting", last_error=None)
    model_bundle = _load_model()
    with _STATUS_LOCK:
        _STATUS["model"] = model_bundle[2]
    spark = (SparkSession.builder.appName("CryptoSentinelStructuredStreaming")
             .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
             .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
             .getOrCreate())
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    parsed = _parsed_input(spark, market_topic, brokers, max_offsets)
    valid = parsed.filter(F.col("_valid"))
    invalid = parsed.filter(~F.col("_valid"))
    historical = valid.filter(F.col("source") == "HISTORICAL_BENCHMARK")
    hist_query = (historical.writeStream.queryName("cryptosentinel-historical-xgboost")
                  .foreachBatch(lambda batch, bid: _process_history(spark, batch, bid, brokers, output_topic, model_bundle))
                  .option("checkpointLocation", str(checkpoint / "historical"))
                  .outputMode("append").start())
    live_trades = valid.filter(F.col("source") == "BINANCE_SPOT")
    bars = (live_trades.withWatermark("event_time", os.getenv("SPARK_WATERMARK_DELAY", "30 seconds"))
            .groupBy(F.window("event_time", "5 seconds").alias("window"), "symbol")
            .agg(F.min_by("price", "event_time").alias("open"),
                 F.max("price").alias("high"), F.min("price").alias("low"),
                 F.max_by("price", "event_time").alias("close"),
                 F.sum("volume").alias("base_volume"),
                 F.sum(F.coalesce(F.col("quote_volume"), F.col("price") * F.col("volume"))).alias("quote_volume"),
                 F.count(F.lit(1)).alias("trade_count"),
                 F.sum(F.when(~F.coalesce(F.col("buyer_is_maker"), F.lit(False)), F.col("volume")).otherwise(F.lit(0.0))).alias("taker_buy_base_volume"))
            .select("symbol", F.col("window.start").alias("window_start"), "open", "high", "low", "close",
                    "base_volume", "quote_volume", "trade_count", "taker_buy_base_volume"))
    live_query = (bars.writeStream.queryName("cryptosentinel-live-five-second-bars")
                  .foreachBatch(lambda batch, bid: _process_live_bars(spark, batch, bid, brokers, output_topic))
                  .option("checkpointLocation", str(checkpoint / "live-bars"))
                  .outputMode("append").start())
    invalid_query = (invalid.writeStream.queryName("cryptosentinel-invalid-records")
                     .foreachBatch(lambda batch, bid: _process_invalid(spark, batch, bid, brokers, dead_letter_topic, output_topic))
                     .option("checkpointLocation", str(checkpoint / "invalid"))
                     .outputMode("append").start())
    queries = {"historical_xgboost": hist_query, "live_bars": live_query, "invalid_records": invalid_query}
    with _STATUS_LOCK:
        _STATUS.update(status="healthy", queries={key: {"id": str(query.id), "active": query.isActive}
                                                   for key, query in queries.items()})
    LOG.info("Spark streaming started: broker=%s market_topic=%s output_topic=%s; model_loaded=%s",
             brokers, market_topic, output_topic, model_bundle[2].get("loaded"))
    try:
        while any(query.isActive for query in queries.values()):
            with _STATUS_LOCK:
                _STATUS["status"] = "processing" if _STATUS["last_batch_at"] else "healthy"
                _STATUS["queries"] = {key: {"id": str(query.id), "active": query.isActive}
                                       for key, query in queries.items()}
            for query in queries.values():
                exc = query.exception() if not query.isActive else None
                if exc:
                    raise RuntimeError(str(exc))
            time.sleep(2)
    except KeyboardInterrupt:
        LOG.info("Stopping Spark streaming queries")
    except Exception as exc:
        with _STATUS_LOCK:
            _STATUS.update(status="unavailable", last_error=str(exc)[:500])
        LOG.exception("Spark streaming worker failed")
        raise
    finally:
        for query in queries.values():
            try:
                query.stop()
                query.awaitTermination(15)
            except Exception:
                LOG.exception("Spark query shutdown failed")
        spark.stop()
        health.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
