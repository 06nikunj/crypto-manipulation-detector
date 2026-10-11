"""
Flask REST API
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Aanya (005) — Visualization & System Evaluation

Provides all REST endpoints consumed by the dashboard.
Serves the dashboard HTML from frontend/index.html.
Integrates outputs from all four members into a single cohesive application:
  - Member 1 (Nikunj): dataset info and status
  - Member 2 (Sruthi): stream status and processed events
  - Member 3 (Arnav):  graph nodes, edges, detection results, alerts
  - Member 4 (Aanya):  evaluation metrics and this API itself
"""

import json
import sys
import time
import math
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS

# ── lazy imports — only loaded after startup ───────────────────────────────────
_ingestion_loaded   = False
_pipeline_loaded    = False
_graph_loaded       = False
_stream             = None
_pipeline           = None
_graph_writer       = None
_DATASET_CACHE      = {}

def _query_int(name, default, minimum=1, maximum=5000):
    try:
        return max(minimum, min(int(request.args.get(name, default)), maximum))
    except (TypeError, ValueError):
        return default

def _query_float(name, default=0.0):
    try:
        value = float(request.args.get(name, default))
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default

def _filter_date_range(items, timestamp_key):
    start = request.args.get("from", "").strip()
    end = request.args.get("to", "").strip()
    try:
        start = datetime.strptime(start, "%Y-%m-%d").date().isoformat() if start else ""
        end = datetime.strptime(end, "%Y-%m-%d").date().isoformat() if end else ""
    except ValueError:
        return items, "Date bounds must use YYYY-MM-DD."
    if start and end and start > end:
        return items, "Start date must not be after end date."
    if start:
        items = [item for item in items if str(item.get(timestamp_key) or "")[:10] >= start]
    if end:
        items = [item for item in items if str(item.get(timestamp_key) or "")[:10] <= end]
    return items, None


def _ensure_pipeline():
    global _stream, _pipeline, _graph_writer
    global _ingestion_loaded, _pipeline_loaded, _graph_loaded
    if not _ingestion_loaded:
        from backend.spark.src.ingestion import stream as s
        _stream = s
        _ingestion_loaded = True
    if not _pipeline_loaded:
        from backend.spark.src.anomaly_detection import pipeline as p
        _pipeline = p
        _pipeline_loaded = True
    if not _graph_loaded:
        from backend.spark.src.graph_builder import graph_writer as gw
        _graph_writer = gw
        _graph_loaded = True


app = Flask(__name__, static_folder=str(ROOT / "frontend"))
CORS(app)


# ── serve dashboard ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(ROOT / "frontend"), "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(str(ROOT / "frontend"), filename)


# ── health ─────────────────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({
        "status"   : "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataset"  : _dataset_info(),
        "model"    : _model_info(),
    })


# ── stream controls ────────────────────────────────────────────────────────────

@app.route("/api/stream/start", methods=["POST"])
def stream_start():
    _ensure_pipeline()
    data  = request.get_json(silent=True) or {}
    try:
        speed = float(data.get("speed", 500.0))
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"ok": False, "msg": "speed must be a positive number"}), 400

    result = _stream.start(speed=speed)
    if result["ok"]:
        if not _pipeline._thread or not _pipeline._thread.is_alive():
            _pipeline.start()
        if not _graph_writer._thread or not _graph_writer._thread.is_alive():
            _graph_writer.start()
    return jsonify(result)


@app.route("/api/stream/pause", methods=["POST"])
def stream_pause():
    _ensure_pipeline()
    return jsonify(_stream.pause())


@app.route("/api/stream/resume", methods=["POST"])
def stream_resume():
    _ensure_pipeline()
    return jsonify(_stream.resume())


@app.route("/api/stream/stop", methods=["POST"])
def stream_stop():
    _ensure_pipeline()
    return jsonify(_stream.stop())


@app.route("/api/stream/speed", methods=["POST"])
def stream_speed():
    _ensure_pipeline()
    data  = request.get_json(silent=True) or {}
    try:
        speed = float(data.get("speed", 500.0))
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"ok": False, "msg": "speed must be a positive number"}), 400
    return jsonify(_stream.set_speed(speed))


@app.route("/api/stream/status")
def stream_status():
    _ensure_pipeline()
    state = _stream.get_state()
    state["pipeline"] = _pipeline.get_state()
    state["graph"]    = _graph_writer.get_state()
    return jsonify(state)


# ── events ─────────────────────────────────────────────────────────────────────

@app.route("/api/events")
def get_events():
    _ensure_pipeline()
    from backend.spark.src.anomaly_detection import get_recent_events
    limit  = _query_int("limit", 100, 1, 500)
    offset = _query_int("offset", 0, 0, 2000)
    symbol = request.args.get("symbol", "").upper().strip()
    label  = request.args.get("label", "").strip()       # PUMP_AND_DUMP | NORMAL
    min_risk = max(0.0, min(1.0, _query_float("min_risk", 0.0)))
    query = request.args.get("q", "").strip().casefold()

    events = get_recent_events(2000)   # fetch full ring buffer then filter

    if symbol:
        events = [e for e in events if e.get("symbol", "") == symbol]
    if label:
        events = [e for e in events if e.get("label", "") == label]
    if min_risk > 0:
        events = [e for e in events if (e.get("risk_score") or 0) >= min_risk]
    if query:
        events = [e for e in events if query in str(e.get("event_id", "")).casefold()
                  or query in str(e.get("symbol", "")).casefold()]
    events, date_error = _filter_date_range(events, "event_timestamp")
    if date_error:
        return jsonify({"error": date_error}), 400
    sort = request.args.get("sort", "timestamp").strip().casefold()
    sort_field = {"timestamp": "event_timestamp", "risk": "risk_score", "symbol": "symbol"}.get(sort, "event_timestamp")
    descending = request.args.get("order", "desc").strip().casefold() != "asc"
    if sort_field == "risk_score":
        events.sort(key=lambda e: float(e.get(sort_field) or 0), reverse=descending)
    else:
        events.sort(key=lambda e: str(e.get(sort_field) or "").casefold(), reverse=descending)
    page = events[offset:offset + limit]
    return jsonify({"events": page, "count": len(events), "returned": len(page),
                    "offset": offset, "has_more": offset + len(page) < len(events),
                    "scope": "recent_in_memory", "window_size": 2000})


@app.route("/api/events/summary")
def events_summary():
    _ensure_pipeline()
    from backend.spark.src.anomaly_detection import get_recent_events, RECENT_EVENTS

    # Use last 500 in-memory events for symbol breakdown only (these are used for
    # price/volume display; they do NOT represent totals).
    events = get_recent_events(500)

    # Price/volume by symbol — derived from recent in-memory window only
    by_symbol: dict = {}
    for e in events:
        sym = e.get("symbol", "UNK")
        if sym not in by_symbol:
            by_symbol[sym] = {"prices": [], "volumes": [], "timestamps": []}
        by_symbol[sym]["prices"].append(e.get("price", 0))
        by_symbol[sym]["volumes"].append(e.get("volume", 0))
        by_symbol[sym]["timestamps"].append(e.get("event_timestamp", ""))

    symbols_summary = {
        sym: {
            "latest_price" : round(vals["prices"][-1], 8) if vals["prices"] else 0,
            "avg_volume"   : round(sum(vals["volumes"]) / max(len(vals["volumes"]), 1), 4),
            "event_count"  : len(vals["prices"]),
            "latest_timestamp": vals["timestamps"][-1] if vals["timestamps"] else None,
        }
        for sym, vals in by_symbol.items()
    }

    stream_state   = _stream.get_state()
    pipeline_state = _pipeline.get_state()
    from backend.spark.src.graph_builder import get_graph_stats
    graph_state = get_graph_stats()

    # ── Authoritative counters (from pipeline, not in-memory window) ──────────
    total_processed    = pipeline_state["processed"]        # cumulative chunks classified
    total_alerts       = pipeline_state["alerts_generated"] # cumulative alerts fired
    total_normal       = pipeline_state["normal_events"]

    return jsonify({
        # Authoritative totals from the pipeline state machine
        "total_processed"  : total_processed,
        "total_alerts"     : total_alerts,        # renamed from pump_detected (was double-counting)
        "total_normal"     : total_normal,         # corrected: total - alerts
        "total_errors"     : pipeline_state["errors"],
        "total_pump_predictions": pipeline_state["pump_predictions"],
        "total_in_memory"  : len(RECENT_EVENTS),  # bounded ring buffer (≤ 2000)
        # Stream state
        "events_per_sec"   : stream_state["events_per_sec"],
        "stream_status"    : stream_state["status"],
        "replay_speed"     : stream_state["replay_speed"],
        "replay_position"  : stream_state["position"],
        "replay_total"     : stream_state["total"],
        "replay_id"        : stream_state["replay_id"],
        "latest_event_ts"  : stream_state["latest_event_ts"],
        "elapsed_seconds"  : stream_state["elapsed_seconds"],
        "queue_size"       : stream_state["queue_size"],
        "dataset_rows"     : _dataset_info().get("rows", 0),
        "processing_errors": pipeline_state["errors"],
        "model_loaded"     : pipeline_state["model_loaded"],
        "graph"            : graph_state,
        # Symbol breakdown (recent in-memory window)
        "symbols"          : symbols_summary,
    })


# ── alerts ─────────────────────────────────────────────────────────────────────

@app.route("/api/alerts")
def get_alerts():
    _ensure_pipeline()
    from backend.spark.src.anomaly_detection import get_recent_alerts
    limit  = _query_int("limit", 50, 1, 500)
    alerts = get_recent_alerts(limit)
    return jsonify({"alerts": alerts, "count": len(alerts)})


@app.route("/api/alerts/<alert_id>/acknowledge", methods=["POST"])
def acknowledge_alert(alert_id: str):
    _ensure_pipeline()
    from backend.spark.src.anomaly_detection import ALERTS, ALERTS_LOCK
    with ALERTS_LOCK:
        for alert in ALERTS:
            if alert.get("alert_id") == alert_id:
                alert["acknowledged"] = True
                return jsonify({"ok": True})
    return jsonify({"ok": False, "msg": "Alert not found"}), 404


@app.route("/api/alerts/episodes")
def alert_episodes():
    """
    Return pump episodes grouped by unique pump event_id.

    Each episode = one pump window (event_id like SYM_DATE_TIME_pump).
    Within a 600-second pump window, many 5-second chunks are flagged — each
    chunk produces one alert entry. This endpoint deduplicates them into one
    entry per pump episode, exposes the chunk count, and picks the highest-risk
    chunk's explanation.

    This is distinct from /api/alerts which returns all individual chunk alerts.
    """
    _ensure_pipeline()
    from backend.spark.src.anomaly_detection import ALERT_EPISODES, ALERTS_LOCK
    with ALERTS_LOCK:
        episodes = [{k: v for k, v in ep.items() if not k.startswith("_")}
                    for ep in ALERT_EPISODES.values()]

    # Sort by detection time descending
    episodes.sort(key=lambda e: e.get("detected_at", ""), reverse=True)
    total = len(episodes)
    symbol = request.args.get("symbol", "").strip().casefold()
    query = request.args.get("q", "").strip().casefold()
    min_risk = max(0.0, min(1.0, _query_float("min_risk", 0.0)))
    if symbol:
        episodes = [e for e in episodes if symbol in str(e.get("symbol", "")).casefold()]
    if query:
        episodes = [e for e in episodes if query in str(e.get("symbol", "")).casefold()
                    or query in str(e.get("episode_id", "")).casefold()]
    if min_risk:
        episodes = [e for e in episodes if e.get("peak_risk_score", 0) >= min_risk]
    episodes, date_error = _filter_date_range(episodes, "timestamp")
    if date_error:
        return jsonify({"error": date_error}), 400
    ack_filter = request.args.get("ack", "all").strip().casefold()
    if ack_filter == "unack":
        episodes = [e for e in episodes if not e.get("acknowledged")]
    elif ack_filter == "ack":
        episodes = [e for e in episodes if e.get("acknowledged")]
    filtered_count = len(episodes)
    total_alert_chunks = sum(int(e.get("flagged_chunks", 0)) for e in episodes)
    limit = _query_int("limit", 100, 1, 1000)
    offset = _query_int("offset", 0, 0, 100000)
    page = episodes[offset:offset + limit]
    return jsonify({"episodes": page, "total_episodes": total,
                    "filtered_episodes": filtered_count, "total_alert_chunks": total_alert_chunks,
                    "offset": offset, "returned": len(page),
                    "has_more": offset + len(page) < filtered_count})

@app.route("/api/alerts/episodes/<path:episode_id>/acknowledge", methods=["POST"])
def acknowledge_episode(episode_id: str):
    _ensure_pipeline()
    from backend.spark.src.anomaly_detection import ALERT_EPISODES, ALERTS, ALERTS_LOCK
    with ALERTS_LOCK:
        ep = ALERT_EPISODES.get(episode_id)
        if not ep:
            return jsonify({"ok": False, "msg": "Episode not found"}), 404
        ep["acknowledged"] = True
        ids = set(ep.get("all_alert_ids", []))
        for alert in ALERTS:
            if alert.get("alert_id") in ids:
                alert["acknowledged"] = True
    return jsonify({"ok": True})


# ── graph ──────────────────────────────────────────────────────────────────────

@app.route("/api/graph/nodes")
def graph_nodes():
    _ensure_pipeline()
    from backend.spark.src import graph_builder as gb
    limit = _query_int("limit", 200, 1, 300)
    nodes = gb.get_all_nodes(limit)
    edges = gb.get_all_edges(limit * 3)
    return jsonify({"nodes": nodes, "edges": edges,
                    "stats": gb.get_graph_stats()})


@app.route("/api/graph/suspicious")
def graph_suspicious():
    _ensure_pipeline()
    from backend.spark.src import graph_builder as gb
    limit = _query_int("limit", 100, 1, 300)
    sus   = gb.get_suspicious_events(limit)
    return jsonify({"suspicious": sus, "count": len(sus)})


@app.route("/api/graph/stats")
def graph_stats():
    _ensure_pipeline()
    from backend.spark.src import graph_builder as gb
    return jsonify(gb.get_graph_stats())


@app.route("/api/graph/neighbors/<node_id>")
def graph_neighbors(node_id: str):
    _ensure_pipeline()
    from backend.spark.src import graph_builder as gb
    depth = _query_int("depth", 2, 1, 5)
    return jsonify(gb.get_neighbors(node_id, depth))


@app.route("/api/graph/asset/<symbol>")
def graph_asset(symbol: str):
    _ensure_pipeline()
    from backend.spark.src import graph_builder as gb
    return jsonify(gb.get_asset_subgraph(symbol.upper()))


# ── model evaluation ───────────────────────────────────────────────────────────

@app.route("/api/evaluation")
def evaluation():
    metrics_path = ROOT / "models" / "eval_metrics.json"
    if not metrics_path.exists():
        return jsonify({
            "available": False,
            "message"  : "No evaluation metrics found. Run scripts/train_models.py first."
        })
    metrics = json.loads(metrics_path.read_text())
    try:
        tn, fp = metrics["confusion_matrix"][0]
        fn, tp = metrics["confusion_matrix"][1]
        div = lambda n, d: (n / d) if d else 0.0
        metrics["accuracy"] = div(tn + tp, tn + fp + fn + tp)
        metrics["specificity"] = div(tn, tn + fp)
        metrics["per_class"] = {
            "normal": {"precision": div(tn, tn + fn), "recall": div(tn, tn + fp),
                       "f1": div(2 * tn, 2 * tn + fp + fn), "support": tn + fp},
            "pump": {"precision": div(tp, tp + fp), "recall": div(tp, tp + fn),
                     "f1": div(2 * tp, 2 * tp + fp + fn), "support": fn + tp},
        }
    except (KeyError, TypeError, ValueError, IndexError):
        pass
    metrics["available"] = True
    return jsonify(metrics)


# ── dataset info ───────────────────────────────────────────────────────────────

@app.route("/api/dataset")
def dataset_info():
    return jsonify(_dataset_info())


# ── helpers ────────────────────────────────────────────────────────────────────

def _dataset_info() -> dict:
    data_dir = ROOT / "data" / "pump_dump_dataset"
    labeled  = data_dir / "labeled_trades.csv"
    events   = data_dir / "pump_events_clean.csv"
    info = {
        "name"    : "SystemsLab-Sapienza Pump-and-Dump Dataset",
        "source"  : "https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset",
        "paper"   : "Pump and Dumps in the Bitcoin Era, ICCCN 2020",
        "ready"   : labeled.exists(),
    }
    if labeled.exists():
        try:
            import pandas as pd
            stamp = labeled.stat().st_mtime
            if _DATASET_CACHE.get("mtime") != stamp:
                full = pd.read_csv(labeled)
                labels = pd.to_numeric(full["is_pump"], errors="coerce").fillna(0)
                timestamps = pd.to_datetime(full.get("timestamp"), errors="coerce", utc=True)
                _DATASET_CACHE.update(mtime=stamp, rows=len(full), pumps=int((labels == 1).sum()),
                                      normal=int((labels == 0).sum()), symbols=int(full.symbol.nunique()),
                                      windows=int(full.event_id.nunique()), columns=list(full.columns),
                                      start_date=timestamps.min().isoformat() if timestamps.notna().any() else None,
                                      end_date=timestamps.max().isoformat() if timestamps.notna().any() else None,
                                      missing_cells=int(full.isna().sum().sum()),
                                      missing_by_column={k: int(v) for k, v in full.isna().sum().items()},
                                      exchanges=int(full.exchange.nunique()) if "exchange" in full.columns else 0)
            info.update({k: v for k, v in _DATASET_CACHE.items() if k != "mtime"})
        except Exception:
            info["error"] = "Dataset file exists but could not be parsed"
    if events.exists():
        try:
            import pandas as pd
            ev = pd.read_csv(events)
            info["pump_events"] = len(ev)
            info["pump_event_symbols"] = ev["symbol"].nunique() if "symbol" in ev.columns else 0
        except Exception:
            pass
    return info


def _model_info() -> dict:
    model_path   = ROOT / "models" / "xgboost_detector.joblib"
    metrics_path = ROOT / "models" / "eval_metrics.json"
    if not model_path.exists():
        return {"available": False}
    info = {"available": True}
    if metrics_path.exists():
        m = json.loads(metrics_path.read_text())
        info.update({
            "precision": m.get("precision"),
            "recall"   : m.get("recall"),
            "f1"       : m.get("f1"),
            "pr_auc"   : m.get("pr_auc"),
        })
    return info


# ── run ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5001))
    print("=" * 60)
    print("  Crypto Manipulation Detector — API Server")
    print(f"  Dashboard: http://localhost:{port}")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
