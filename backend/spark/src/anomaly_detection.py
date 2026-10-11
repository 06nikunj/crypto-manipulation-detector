"""
Anomaly Detection & Risk Scoring Pipeline
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Arnav (044) — Graph Analysis & Detection

Consumes feature-enriched events from Sruthi's stream (Member 2),
runs XGBoost inference, assigns risk scores, generates alerts,
and pushes enriched events to the graph writer and to the API
consumed by Aanya's dashboard (Member 4).

Rule-based fallback is used when the XGBoost model is not yet trained.
"""

import threading
import queue
import time
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
import numpy as np

ROOT = Path(__file__).parent.parent.parent.parent

# ── output queues consumed downstream ─────────────────────────────────────────
ENRICHED_QUEUE: queue.Queue = queue.Queue(maxsize=5000)
ALERT_OUTPUT_QUEUE: queue.Queue = queue.Queue(maxsize=1000)

# Keep a bounded in-memory store of recent enriched events (for REST API)
RECENT_EVENTS: list = []
RECENT_EVENTS_LOCK = threading.Lock()
MAX_RECENT = 2000

ALERTS: list = []
ALERTS_LOCK = threading.Lock()
MAX_ALERTS = 500
ALERT_EPISODES: dict[str, dict] = {}

# ── per-symbol rolling history (for feature extraction) ───────────────────────
_symbol_history: dict[str, list] = {}
_history_lock = threading.Lock()
HISTORY_SIZE = 10     # last N chunks per symbol

RISK_THRESHOLD = 0.50


class DetectionPipeline:
    """
    Runs the feature extraction + XGBoost inference loop in a background thread.

    Reads raw events from `ingestion.EVENT_QUEUE`, enriches them, writes to
    `ENRICHED_QUEUE`, and generates alerts for high-risk events.
    """

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop   = threading.Event()
        self._model  = None
        self._scaler = None
        self.processed = 0
        self.errors    = 0
        self.alerts_generated = 0
        self.normal_events = 0
        self.pump_predictions = 0
        self._replay_id = None

    def start(self):
        self._stop.clear()
        self._load_model()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="DetectionPipeline"
        )
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _load_model(self):
        """Load XGBoost model + scaler. Fall back to rule-based if unavailable."""
        import joblib
        model_path  = ROOT / "models" / "xgboost_detector.joblib"
        scaler_path = ROOT / "models" / "scaler.joblib"
        if model_path.exists() and scaler_path.exists():
            try:
                self._model  = joblib.load(model_path)
                self._scaler = joblib.load(scaler_path)
                print("  [Pipeline] XGBoost model loaded ✓")
                return
            except Exception as e:
                print(f"  [Pipeline] Model load error: {e} — using rule-based fallback")
        print("  [Pipeline] No trained model found — using rule-based detector")

    def _loop(self):
        from backend.spark.src.ingestion import EVENT_QUEUE
        from backend.spark.src.preprocessing import (
            extract_single_event_features, FEATURE_COLS
        )

        while not self._stop.is_set():
            try:
                event = EVENT_QUEUE.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                enriched = self._process_event(event, FEATURE_COLS,
                                               extract_single_event_features)
            except Exception as exc:
                self.errors += 1
                enriched = {**event, "error": str(exc),
                            "risk_score": 0.0, "label": "ERROR",
                            "predicted_class": 0}

            # Store in recent events
            with RECENT_EVENTS_LOCK:
                RECENT_EVENTS.append(enriched)
                if len(RECENT_EVENTS) > MAX_RECENT:
                    RECENT_EVENTS.pop(0)

            # Push to enriched queue (for graph writer)
            try:
                ENRICHED_QUEUE.put_nowait(enriched)
            except queue.Full:
                pass

            # Generate alert if high-risk
            if enriched.get("risk_score", 0) >= RISK_THRESHOLD:
                alert = _make_alert(enriched)
                with ALERTS_LOCK:
                    episode_id = str(alert.get("event_id") or alert["alert_id"])
                    ep = ALERT_EPISODES.get(episode_id)
                    if ep is None:
                        ep = {"episode_id": episode_id, "symbol": alert["symbol"],
                              "peak_risk_score": alert["risk_score"], "avg_risk_score": alert["risk_score"],
                              "flagged_chunks": 0, "timestamp": alert["timestamp"],
                              "detected_at": alert["detected_at"], "explanation": alert["explanation"],
                              "ground_truth": alert["ground_truth"], "label": alert["label"],
                              "acknowledged": False, "all_alert_ids": [], "_risk_sum": 0.0}
                        ALERT_EPISODES[episode_id] = ep
                    ep["flagged_chunks"] += 1
                    ep["_risk_sum"] += float(alert["risk_score"] or 0)
                    ep["avg_risk_score"] = round(ep["_risk_sum"] / ep["flagged_chunks"], 4)
                    ep["all_alert_ids"].append(alert["alert_id"])
                    if alert["risk_score"] >= ep["peak_risk_score"]:
                        ep.update(peak_risk_score=alert["risk_score"], timestamp=alert["timestamp"],
                                  detected_at=alert["detected_at"], explanation=alert["explanation"],
                                  ground_truth=alert["ground_truth"])
                    ALERTS.append(alert)
                    if len(ALERTS) > MAX_ALERTS:
                        ALERTS.pop(0)
                try:
                    ALERT_OUTPUT_QUEUE.put_nowait(alert)
                except queue.Full:
                    pass
                self.alerts_generated += 1
                self.pump_predictions += 1
            elif enriched.get("label") == "NORMAL":
                self.normal_events += 1

            self.processed += 1
            EVENT_QUEUE.task_done()

    def _process_event(self, event: dict, feature_cols: list,
                       extract_fn) -> dict:
        symbol = event["symbol"]

        replay_id = event.get("replay_id")
        if replay_id and replay_id != self._replay_id:
            with _history_lock:
                _symbol_history.clear()
            self._replay_id = replay_id

        # Update history
        with _history_lock:
            hist = _symbol_history.setdefault(symbol, [])
            history_snapshot = list(hist[-HISTORY_SIZE:])
            hist.append(event)
            if len(hist) > HISTORY_SIZE * 2:
                _symbol_history[symbol] = hist[-HISTORY_SIZE:]

        # Extract features
        features = extract_fn(event, history_snapshot)

        # Run model or rule-based
        if self._model is not None:
            result = self._model_predict(features, feature_cols)
        else:
            result = self._rule_based(features)

        enriched = {
            **event,
            **features,
            **result,
        }
        return enriched

    def _model_predict(self, features: dict, feature_cols: list) -> dict:
        """XGBoost inference."""
        import joblib
        x = np.array([[features.get(c, 0.0) for c in feature_cols]])
        if self._scaler is not None:
            x = self._scaler.transform(x)
        proba = float(self._model.predict_proba(x)[0, 1])
        predicted = int(proba >= RISK_THRESHOLD)

        importances = self._model.feature_importances_
        top = sorted(
            [(feature_cols[i], float(features.get(feature_cols[i], 0)),
              float(importances[i]))
             for i in range(len(feature_cols))],
            key=lambda t: t[2], reverse=True
        )[:4]
        explanation = [
            {"feature": f, "value": round(v, 6), "importance": round(imp, 4)}
            for f, v, imp in top
        ]

        return {
            "predicted_class": predicted,
            "risk_score"      : round(proba, 4),
            "label"           : "PUMP_AND_DUMP" if predicted else "NORMAL",
            "explanation"     : explanation,
        }

    def _rule_based(self, features: dict) -> dict:
        """
        Simple rule-based fallback detector.
        Flags events where rush_order_ratio > 0.5 AND anomaly_score > 2.
        """
        rush = features.get("rush_order_ratio", 0)
        anomaly = features.get("anomaly_score", 0)
        vol_spike = features.get("volume_change_pct", 0)
        price_spike = features.get("price_change_pct", 0)

        # Weighted score
        score = (
            rush * 0.40
            + min(anomaly / 5, 1) * 0.25
            + min(vol_spike / 10, 1) * 0.20
            + min(abs(price_spike) / 0.10, 1) * 0.15
        )
        score = max(0.0, min(score, 1.0))
        predicted = int(score >= RISK_THRESHOLD)

        explanation = [
            {"feature": "rush_order_ratio", "value": round(rush, 4),
             "importance": 0.40},
            {"feature": "anomaly_score",    "value": round(anomaly, 4),
             "importance": 0.25},
            {"feature": "volume_change_pct","value": round(vol_spike, 4),
             "importance": 0.20},
            {"feature": "price_change_pct", "value": round(price_spike, 4),
             "importance": 0.15},
        ]

        return {
            "predicted_class": predicted,
            "risk_score"      : round(float(score), 4),
            "label"           : "PUMP_AND_DUMP" if predicted else "NORMAL",
            "explanation"     : explanation,
        }

    def get_state(self) -> dict:
        return {
            "processed"        : self.processed,
            "errors"           : self.errors,
            "alerts_generated" : self.alerts_generated,
            "normal_events"    : self.normal_events,
            "pump_predictions" : self.pump_predictions,
            "model_loaded"     : self._model is not None,
        }


# ── alert builder ──────────────────────────────────────────────────────────────

def _make_alert(event: dict) -> dict:
    return {
        "alert_id"    : f"ALT_{event.get('sequence', 0):06d}_{time.time_ns()}",
        "event_id"    : event.get("event_id", ""),
        "symbol"      : event.get("symbol", ""),
        "risk_score"  : event.get("risk_score", 0),
        "label"       : event.get("label", "PUMP_AND_DUMP"),
        "timestamp"   : event.get("event_timestamp", ""),
        "detected_at" : datetime.now(timezone.utc).isoformat(),
        "explanation" : event.get("explanation", []),
        "ground_truth": event.get("ground_truth", -1),
        "acknowledged": False,
    }


def get_recent_alerts(limit: int = 50) -> list:
    with ALERTS_LOCK:
        return list(reversed(ALERTS[-limit:]))


def get_recent_events(limit: int = 100) -> list:
    with RECENT_EVENTS_LOCK:
        return list(reversed(RECENT_EVENTS[-limit:]))


# ── module singleton ───────────────────────────────────────────────────────────
pipeline = DetectionPipeline()


if __name__ == "__main__":
    from backend.spark.src.ingestion import stream
    print("Starting stream + detection pipeline …")
    pipeline.start()
    stream.start(speed=50.0)
    time.sleep(5)
    stream.stop()
    pipeline.stop()
    state = pipeline.get_state()
    print(f"Processed: {state['processed']} | Alerts: {state['alerts_generated']}")
    events = get_recent_events(5)
    for e in events:
        print(f"  {e['symbol']} risk={e.get('risk_score')} label={e.get('label')}")
