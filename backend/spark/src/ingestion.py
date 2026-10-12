"""
Streaming Ingestion — Historical Dataset Replay Engine
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Sruthi (001) — Stream Processing & Feature Engineering

Reads clean events produced by Member 1 (Nikunj / data/download.py) and
replays them as a deterministic event stream through Kafka (when available)
or a local in-memory queue (local demo mode).

Replay modes
------------
  running — streams all records chronologically at adjustable speed
  paused  — holds the stream; no events emitted
  stopped — terminates replay; resets position

Controls are exposed through the module-level StreamController object,
which the Flask API uses to start/pause/resume/stop/reset the stream.
"""

import time
import threading
import queue
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

ROOT          = Path(__file__).parent.parent.parent.parent
DATA_CSV      = ROOT / "data" / "pump_dump_dataset" / "labeled_trades.csv"
RANDOM_SEED   = 42

# ── shared event queue ─────────────────────────────────────────────────────────
# The Flask API reads from this queue to serve SSE / polling endpoints.
EVENT_QUEUE: queue.Queue = queue.Queue(maxsize=5000)

# ── alert queue ───────────────────────────────────────────────────────────────
ALERT_QUEUE: queue.Queue = queue.Queue(maxsize=1000)


class StreamController:
    """
    Thread-safe controller for the historical replay stream.

    Attributes
    ----------
    status          : "stopped" | "running" | "paused"
    replay_speed    : float  multiplier — 1.0 = real-time, 10.0 = 10× faster
    processed       : int    total events processed so far
    errors          : int    total processing errors
    start_time      : float  wall-clock time when stream started
    latest_event_ts : str    ISO string of the latest event timestamp
    """

    def __init__(self):
        self.status           = "stopped"
        self.replay_speed     = 500.0       # fast historical playback; no synthetic events
        self._pause_event     = threading.Event()
        self._stop_event      = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock            = threading.Lock()

        self.processed        = 0
        self.position         = 0
        self.total_rows       = 0
        self.replay_id        = None
        self.errors           = 0
        self.start_time       = None
        self.latest_event_ts  = None
        self.events_per_sec   = 0.0
        self._recent_times    = []          # wall-clock times of last 20 events

    # ── public controls ────────────────────────────────────────────────────────

    def start(self, speed: float = None):
        with self._lock:
            if self.status == "running":
                return {"ok": False, "msg": "Already running"}
            if speed is not None:
                self.replay_speed = max(0.1, min(speed, 1000.0))
            self._stop_event.clear()
            self._pause_event.set()   # not paused
            self.status = "running"
            self.start_time = time.time()
            self.position = 0
            self.replay_id = uuid.uuid4().hex[:10]
            self._thread = threading.Thread(
                target=self._replay_loop, daemon=True, name="ReplayStream"
            )
            self._thread.start()
        return {"ok": True, "msg": "Stream started"}

    def pause(self):
        with self._lock:
            if self.status != "running":
                return {"ok": False, "msg": "Not running"}
            self._pause_event.clear()
            self.status = "paused"
        return {"ok": True, "msg": "Stream paused"}

    def resume(self):
        with self._lock:
            if self.status != "paused":
                return {"ok": False, "msg": "Not paused"}
            self._recent_times.clear()
            self.events_per_sec = 0.0
            self._pause_event.set()
            self.status = "running"
        return {"ok": True, "msg": "Stream resumed"}

    def stop(self):
        with self._lock:
            self._stop_event.set()
            self._pause_event.set()   # unblock if paused
            self.status = "stopped"
        if self._thread:
            self._thread.join(timeout=3)
        return {"ok": True, "msg": "Stream stopped"}

    def set_speed(self, speed: float):
        self.replay_speed = max(0.1, min(speed, 1000.0))
        return {"ok": True, "speed": self.replay_speed}

    def get_state(self) -> dict:
        elapsed = time.time() - self.start_time if self.start_time else 0
        return {
            "status"          : self.status,
            "replay_speed"    : self.replay_speed,
            "processed"       : self.processed,
            "position"        : self.position,
            "total"           : self.total_rows,
            "replay_id"       : self.replay_id,
            "errors"          : self.errors,
            "elapsed_seconds" : round(elapsed, 1),
            "events_per_sec"  : round(self.events_per_sec if self.status == "running" else 0.0, 2),
            "latest_event_ts" : self.latest_event_ts,
            "queue_size"      : EVENT_QUEUE.qsize(),
        }

    # ── internal replay loop ───────────────────────────────────────────────────

    def _replay_loop(self):
        """Load dataset and emit events into EVENT_QUEUE.
        
        Preserve the benchmark's timestamp order exactly. Do not shuffle or
        stratify by the ground-truth label: chronological ordering is required
        for rolling per-symbol features to behave like the training pipeline.
        The original label is carried only for retrospective benchmark review;
        the inference path selects model features separately.
        """
        try:
            df = _load_dataset()
        except Exception as exc:
            self.errors += 1
            _put_alert({
                "type"   : "SYSTEM_ERROR",
                "message": f"Failed to load dataset: {exc}",
                "ts"     : _now_iso(),
            })
            with self._lock:
                self.status = "stopped"
            return

        # Preserve benchmark chronology; shuffling/grouping rows corrupts rolling
        # features and makes the historical timestamps misleading.
        df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
        if "event_id" not in df.columns:
            df["event_id"] = "evt_" + df.index.astype(str)
        total = len(df)
        self.total_rows = total
        chunk_seconds = 5.0

        for idx, (_, row) in enumerate(df.iterrows()):
            if self._stop_event.is_set():
                break
            # Block while paused
            self._pause_event.wait()
            if self._stop_event.is_set():
                break

            event = _row_to_event(row, idx, total)
            event["replay_id"] = self.replay_id
            self.position = idx + 1

            # Try to put into queue; drop if full (backpressure)
            try:
                EVENT_QUEUE.put_nowait(event)
            except queue.Full:
                self.errors += 1
            else:
                self.processed += 1
                self.latest_event_ts = event["event_timestamp"]

            # Track events per second
            now = time.time()
            self._recent_times.append(now)
            if len(self._recent_times) > 20:
                self._recent_times.pop(0)
            if len(self._recent_times) >= 2:
                span = self._recent_times[-1] - self._recent_times[0]
                self.events_per_sec = (
                    (len(self._recent_times) - 1) / span if span > 0 else 0
                )

            # Recalculate sleep dynamically (speed may change mid-stream)
            self._stop_event.wait(chunk_seconds / max(self.replay_speed, 0.01))

        # Stream finished
        with self._lock:
            if not self._stop_event.is_set():
                self.status = "stopped"


# ── helpers ────────────────────────────────────────────────────────────────────

def _load_dataset() -> pd.DataFrame:
    if not DATA_CSV.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_CSV}\nRun `python3 data/download.py` first."
        )
    df = pd.read_csv(DATA_CSV)
    required = {"timestamp", "symbol", "price", "volume", "n_trades", "rush_orders", "buy_ratio", "is_pump"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError("Dataset is missing required columns: " + ", ".join(sorted(missing)))
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp", "symbol"])
    for col in ("price", "volume", "n_trades", "rush_orders", "buy_ratio", "is_pump"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
    return df


def _row_to_event(row: pd.Series, idx: int, total: int) -> dict:
    """Convert a dataset row to a structured event dict."""
    ts = row["timestamp"]
    if hasattr(ts, "isoformat"):
        ts_str = ts.isoformat()
    else:
        ts_str = str(ts)

    return {
        "event_id"         : row.get("event_id", f"evt_{idx}"),
        "sequence"         : int(idx),
        "total"            : int(total),
        "event_timestamp"  : ts_str,
        "processing_timestamp": _now_iso(),
        "symbol"           : str(row.get("symbol", "UNK")),
        "price"            : float(row.get("price", 0)),
        "volume"           : float(row.get("volume", 0)),
        "n_trades"         : int(row.get("n_trades", 0)),
        "rush_orders"      : int(row.get("rush_orders", 0)),
        "buy_ratio"        : float(row.get("buy_ratio", 0.5)),
        "ground_truth"     : int(row.get("is_pump", 0)),
        "group"            : str(row.get("group", "unknown")),
        "exchange"         : str(row.get("exchange", "binance")),
        # Will be enriched by classifier in pipeline.py
        "predicted_class"  : None,
        "risk_score"       : None,
        "label"            : None,
    }


def _put_alert(alert: dict):
    try:
        ALERT_QUEUE.put_nowait(alert)
    except queue.Full:
        pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── module-level singleton ─────────────────────────────────────────────────────
stream = StreamController()


# ── CLI smoke test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting stream at 100× speed for 3 seconds …")
    stream.start(speed=100.0)
    time.sleep(3)
    print(stream.get_state())
    stream.stop()
    print(f"Collected {EVENT_QUEUE.qsize()} events in queue.")
    sample = EVENT_QUEUE.get()
    print("Sample event:", json.dumps(sample, indent=2, default=str))
