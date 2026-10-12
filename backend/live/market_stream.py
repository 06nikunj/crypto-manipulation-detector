"""Public Binance Spot trade stream and live-only analytics.

This deliberately does not pass raw exchange trades into the benchmark XGBoost
model. That model was trained on labeled, engineered five-second Sapienza rows;
its rush-order and other inputs are not validated for Binance live trades.
"""
from __future__ import annotations

import json
import logging
import math
import os
import random
import re
import statistics
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Callable

LOG = logging.getLogger(__name__)
DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
SYMBOL_RE = re.compile(r"^[A-Z0-9]{4,20}$")
BAR_MS = 5_000
MAX_SYMBOLS = 20
MAX_BARS = 360
MAX_TRADES = 1_000
MAX_ALERTS = 500
MAX_GRAPH_EVENTS = 1_000
MAX_SSE_EVENTS = 2_000
PRICE_MOVE_THRESHOLD = 0.01
VOLUME_SPIKE_MULTIPLIER = 3.0
VOLUME_BASELINE_BARS = 20
MODEL_UNAVAILABLE_REASON = (
    "Live trade fields do not reproduce the benchmark's labeled five-second "
    "feature schema (including rush_orders); XGBoost predictions are disabled "
    "for live data until a separate live-data validation is performed."
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def iso_millis(value: int | float | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def configured_symbols() -> list[str]:
    raw = os.getenv("BINANCE_SYMBOLS", ",".join(DEFAULT_SYMBOLS))
    try:
        return normalize_symbols(raw)
    except ValueError as exc:
        LOG.warning("Ignoring invalid BINANCE_SYMBOLS setting: %s", exc)
        return list(DEFAULT_SYMBOLS)


def normalize_symbols(value) -> list[str]:
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple)):
        values = value
    else:
        raise ValueError("symbols must be a comma-separated string or a list")
    symbols = []
    for item in values:
        symbol = str(item).strip().upper()
        if not symbol:
            continue
        if not SYMBOL_RE.fullmatch(symbol):
            raise ValueError(f"Invalid Binance Spot symbol: {symbol[:24]}")
        if symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise ValueError("Choose at least one Spot trading pair")
    if len(symbols) > MAX_SYMBOLS:
        raise ValueError(f"A maximum of {MAX_SYMBOLS} pairs is supported per connection")
    return symbols


class LiveMarketManager:
    """One reconnecting combined-stream connection and its bounded live session."""

    def __init__(self, ws_factory: Callable | None = None, base_url: str | None = None):
        self._ws_factory = ws_factory or self._default_ws_factory
        self._base_url = (base_url or os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443")).rstrip("/")
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws = None
        self._desired_running = False
        self._state = "disconnected"
        self._last_error: str | None = None
        self._connected_at: str | None = None
        self._reconnect_attempts = 0
        self._symbols = configured_symbols()
        self._session_id = uuid.uuid4().hex[:10]
        self._event_count = 0
        self._invalid_messages = 0
        self._last_received_at: str | None = None
        self._last_exchange_event_at: str | None = None
        self._last_trade_at: str | None = None
        self._latest_prices: dict[str, dict] = {}
        self._trades: deque[dict] = deque(maxlen=MAX_TRADES)
        self._bars: dict[str, deque[dict]] = {}
        self._current_bars: dict[str, dict] = {}
        self._alerts: deque[dict] = deque(maxlen=MAX_ALERTS)
        self._nodes: dict[str, dict] = {}
        self._edges: deque[dict] = deque(maxlen=MAX_GRAPH_EVENTS * 2)
        self._graph_order: deque[str] = deque()
        self._last_graph_event: dict[str, str] = {}
        self._sse_events: deque[dict] = deque(maxlen=MAX_SSE_EVENTS)
        self._sequence = 0
        self._trade_listeners: list[Callable[[dict], None]] = []

    @staticmethod
    def _default_ws_factory(url, **callbacks):
        import websocket
        return websocket.WebSocketApp(url, **callbacks)

    @staticmethod
    def _build_url(base_url: str, symbols: list[str]) -> str:
        streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
        return f"{base_url}/stream?streams={streams}"

    def add_trade_listener(self, callback: Callable[[dict], None]) -> None:
        """Register an optional sink, e.g. the separate Kafka bridge."""
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            self._trade_listeners.append(callback)

    def start(self, symbols=None) -> dict:
        try:
            selected = normalize_symbols(symbols if symbols is not None else self._symbols)
        except ValueError as exc:
            return {"ok": False, "error": str(exc)}
        with self._lock:
            alive = bool(self._thread and self._thread.is_alive() and self._desired_running)
            same = selected == self._symbols
            old_thread = self._thread
        if alive and same:
            return {"ok": True, "message": "Already connected or connecting to this watchlist", "status": self.status()}
        if alive:
            self.stop()
        elif old_thread and old_thread.is_alive():
            old_thread.join(timeout=5)
            if old_thread.is_alive():
                return {"ok": False, "error": "The previous WebSocket is still shutting down; retry shortly"}
        with self._lock:
            self._stop_event.clear()
            self._desired_running = True
            self._state = "connecting"
            self._last_error = None
            self._connected_at = None
            self._reconnect_attempts = 0
            self._symbols = selected
            self._session_id = uuid.uuid4().hex[:10]
            self._event_count = 0
            self._invalid_messages = 0
            self._last_received_at = None
            self._last_exchange_event_at = None
            self._last_trade_at = None
            self._latest_prices.clear()
            self._trades.clear()
            self._bars.clear()
            self._current_bars.clear()
            self._alerts.clear()
            self._nodes.clear()
            self._edges.clear()
            self._graph_order.clear()
            self._last_graph_event.clear()
            self._sse_events.clear()
            self._sequence = 0
            self._publish_locked("status", self._connection_payload_locked())
            self._thread = threading.Thread(target=self._run, name="BinanceSpotWebSocket", daemon=True)
            self._thread.start()
        return {"ok": True, "message": "Connection attempt started", "symbols": selected, "status": self.status()}

    def stop(self) -> dict:
        with self._lock:
            self._desired_running = False
            self._stop_event.set()
            self._state = "disconnected"
            self._connected_at = None
            self._publish_locked("status", self._connection_payload_locked())
            ws = self._ws
            thread = self._thread
        if ws is not None:
            try:
                ws.close()
            except Exception:
                LOG.debug("WebSocket close raised while stopping", exc_info=True)
        if thread and thread is not threading.current_thread():
            thread.join(timeout=5)
        with self._lock:
            self._ws = None
            self._thread = None if not (thread and thread.is_alive()) else thread
            self._state = "disconnected"
            self._connected_at = None
        return {"ok": True, "message": "Disconnected from live market data", "status": self.status()}

    def _run(self) -> None:
        delay = 1.0
        while not self._stop_event.is_set():
            with self._lock:
                if self._reconnect_attempts:
                    self._state = "reconnecting"
                else:
                    self._state = "connecting"
                self._publish_locked("status", self._connection_payload_locked())
                url = self._build_url(self._base_url, self._symbols)
            try:
                ws = self._ws_factory(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                with self._lock:
                    self._ws = ws
                ws.run_forever(ping_interval=0)
                if not self._stop_event.is_set():
                    self._set_state("reconnecting", self._last_error or "Connection closed; retrying")
            except Exception as exc:
                if not self._stop_event.is_set():
                    self._set_state("reconnecting", f"WebSocket error: {str(exc)[:240]}")
                    LOG.warning("Binance Spot WebSocket failed; will reconnect: %s", exc)
            finally:
                with self._lock:
                    if self._ws is not None and self._stop_event.is_set():
                        self._ws = None
            if self._stop_event.is_set():
                break
            with self._lock:
                self._reconnect_attempts += 1
            self._stop_event.wait(delay + random.uniform(0, min(0.25, delay / 4)))
            delay = min(30.0, delay * 2)
        self._set_state("disconnected", None)

    def _on_open(self, _ws) -> None:
        with self._lock:
            if self._stop_event.is_set():
                return
            self._state = "connected"
            self._connected_at = utc_now()
            self._last_error = None
            self._publish_locked("status", self._connection_payload_locked())
        LOG.info("Connected to Binance Spot trade streams for %s", ",".join(self._symbols))

    def _on_message(self, _ws, message) -> None:
        self.handle_message(message)

    def _on_error(self, _ws, error) -> None:
        text = str(error)[:240] if error is not None else "Unknown WebSocket error"
        if not self._stop_event.is_set():
            self._set_state("reconnecting", text)
            LOG.warning("Binance Spot WebSocket error: %s", text)

    def _on_close(self, _ws, code=None, reason=None) -> None:
        if self._stop_event.is_set():
            self._set_state("disconnected", None)
        else:
            detail = f"Socket closed ({code})" + (f": {str(reason)[:120]}" if reason else "")
            self._set_state("reconnecting", detail)

    def _set_state(self, state: str, error: str | None) -> None:
        with self._lock:
            self._state = state
            self._last_error = error[:300] if error else None
            if state != "connected":
                self._connected_at = None
            self._publish_locked("status", self._connection_payload_locked())

    def handle_message(self, raw_message) -> bool:
        """Validate and process one Binance raw or combined-stream message."""
        try:
            message = json.loads(raw_message) if isinstance(raw_message, (str, bytes, bytearray)) else raw_message
            if not isinstance(message, dict):
                raise ValueError("message must be a JSON object")
            data = message.get("data", message)
            if not isinstance(data, dict):
                raise ValueError("trade payload must be a JSON object")
            event_type = data.get("e")
            if event_type == "serverShutdown":
                with self._lock:
                    ws = self._ws
                    self._last_error = "Binance announced a stream-server shutdown; reconnecting"
                if ws is not None:
                    ws.close()
                return False
            if event_type != "trade":
                return False
            symbol = str(data.get("s", "")).upper()
            if not SYMBOL_RE.fullmatch(symbol):
                raise ValueError("missing or malformed symbol")
            with self._lock:
                if symbol not in self._symbols:
                    self._invalid_messages += 1
                    self._last_error = f"Ignored trade for unsubscribed symbol {symbol}"
                    return False
            price = float(data.get("p"))
            quantity = float(data.get("q"))
            trade_time = int(data.get("T"))
            event_time = int(data.get("E", trade_time))
            if not (math.isfinite(price) and price > 0 and math.isfinite(quantity) and quantity > 0 and trade_time > 0 and event_time > 0):
                raise ValueError("price, quantity and exchange timestamps must be positive and finite")
            trade = {
                "symbol": symbol,
                "price": price,
                "quantity": quantity,
                "quote_quantity": price * quantity,
                "trade_id": data.get("t"),
                "buyer_is_maker": bool(data.get("m", False)),
                "exchange_event_time": iso_millis(event_time),
                "exchange_trade_time": iso_millis(trade_time),
                "received_at": utc_now(),
            }
        except (ValueError, TypeError, KeyError, json.JSONDecodeError, OverflowError) as exc:
            with self._lock:
                self._invalid_messages += 1
                self._last_error = f"Invalid market message: {str(exc)[:180]}"
                self._publish_locked("status", self._connection_payload_locked())
            return False

        listeners = []
        with self._lock:
            self._event_count += 1
            self._last_received_at = trade["received_at"]
            self._last_exchange_event_at = trade["exchange_event_time"]
            self._last_trade_at = trade["exchange_trade_time"]
            previous = self._latest_prices.get(symbol)
            if previous is None or trade_time >= previous.get("_trade_time_ms", 0):
                self._latest_prices[symbol] = {**trade, "_trade_time_ms": trade_time}
            self._trades.append(dict(trade))
            self._update_bar_locked(trade, trade_time)
            self._publish_locked("trade", trade)
            listeners = list(self._trade_listeners)
        for listener in listeners:
            try:
                listener(dict(trade))
            except Exception:
                LOG.exception("Optional live-trade sink failed")
        return True

    def _update_bar_locked(self, trade: dict, trade_time_ms: int) -> None:
        symbol = trade["symbol"]
        bucket = (trade_time_ms // BAR_MS) * BAR_MS
        current = self._current_bars.get(symbol)
        if current and bucket > current["bucket_start_ms"]:
            self._finalize_bar_locked(current)
            current = None
        if current and bucket < current["bucket_start_ms"]:
            return  # Do not reorder bars or overwrite a newer source timestamp.
        if current is None:
            current = {
                "symbol": symbol,
                "bucket_start_ms": bucket,
                "bucket_start_exchange_time": iso_millis(bucket),
                "open": trade["price"],
                "high": trade["price"],
                "low": trade["price"],
                "close": trade["price"],
                "base_volume": 0.0,
                "quote_volume": 0.0,
                "trade_count": 0,
                "taker_buy_base_volume": 0.0,
                "first_trade_time": trade["exchange_trade_time"],
                "last_trade_time": trade["exchange_trade_time"],
            }
            self._current_bars[symbol] = current
        current["high"] = max(current["high"], trade["price"])
        current["low"] = min(current["low"], trade["price"])
        current["close"] = trade["price"]
        current["base_volume"] += trade["quantity"]
        current["quote_volume"] += trade["quote_quantity"]
        current["trade_count"] += 1
        if not trade["buyer_is_maker"]:
            current["taker_buy_base_volume"] += trade["quantity"]
        current["last_trade_time"] = trade["exchange_trade_time"]

    def _finalize_bar_locked(self, current: dict) -> None:
        symbol = current["symbol"]
        history = self._bars.setdefault(symbol, deque(maxlen=MAX_BARS))
        previous = history[-1] if history else None
        bar = {**current}
        bar["bucket_end_exchange_time"] = iso_millis(current["bucket_start_ms"] + BAR_MS)
        bar["closed_at_received"] = utc_now()
        bar["price_change_pct"] = (bar["close"] / previous["close"] - 1.0) if previous and previous["close"] else None
        baseline = list(history)[-VOLUME_BASELINE_BARS:]
        median_volume = statistics.median(item["base_volume"] for item in baseline) if len(baseline) >= VOLUME_BASELINE_BARS else None
        bar["volume_baseline_bars"] = len(baseline)
        bar["volume_median_previous_20"] = median_volume
        bar["volume_ratio_vs_median"] = (bar["base_volume"] / median_volume) if median_volume and median_volume > 0 else None
        bar["rule_signals"] = []
        if bar["price_change_pct"] is not None and abs(bar["price_change_pct"]) >= PRICE_MOVE_THRESHOLD:
            bar["rule_signals"].append("rapid_price_change")
        if bar["volume_ratio_vs_median"] is not None and bar["volume_ratio_vs_median"] >= VOLUME_SPIKE_MULTIPLIER:
            bar["rule_signals"].append("volume_spike")
        history.append(bar)
        self._add_graph_bar_locked(bar)
        self._publish_locked("bar", bar)
        if bar["rule_signals"]:
            self._emit_rule_alert_locked(bar)
        self._current_bars.pop(symbol, None)

    def _emit_rule_alert_locked(self, bar: dict) -> None:
        signals = bar["rule_signals"]
        parts = []
        if "rapid_price_change" in signals:
            parts.append(f"5-second close-to-close move {bar['price_change_pct'] * 100:+.2f}% met the configured ±{PRICE_MOVE_THRESHOLD * 100:.1f}% rule.")
        if "volume_spike" in signals:
            parts.append(f"5-second base volume was {bar['volume_ratio_vs_median']:.2f}× the median of the previous {VOLUME_BASELINE_BARS} observed five-second bars.")
        severity = "high" if ("volume_spike" in signals and (bar.get("volume_ratio_vs_median") or 0) >= 5) or (abs(bar.get("price_change_pct") or 0) >= 0.02) else "elevated"
        alert = {
            "alert_id": f"LIVE-{self._session_id}-{bar['symbol']}-{bar['bucket_start_ms']}",
            "symbol": bar["symbol"],
            "detected_at": utc_now(),
            "exchange_bar_start": bar["bucket_start_exchange_time"],
            "exchange_bar_end": bar["bucket_end_exchange_time"],
            "last_exchange_trade_at": bar["last_trade_time"],
            "price": bar["close"],
            "base_volume": bar["base_volume"],
            "quote_volume": bar["quote_volume"],
            "trade_count": bar["trade_count"],
            "price_change_pct": bar["price_change_pct"],
            "volume_ratio_vs_median": bar["volume_ratio_vs_median"],
            "detection_method": "rule_based_five_second_market_bar",
            "severity": severity,
            "signals": signals,
            "explanation": " ".join(parts) + " This is a market anomaly warning, not evidence of manipulation or wrongdoing.",
            "model_prediction": "unavailable_live_schema",
        }
        self._alerts.append(alert)
        self._publish_locked("alert", alert)

    def _add_graph_bar_locked(self, bar: dict) -> None:
        symbol = bar["symbol"]
        asset_id = f"live_asset:{symbol}"
        event_id = f"live_market:{self._session_id}:{symbol}:{bar['bucket_start_ms']}"
        self._nodes.setdefault(asset_id, {"id": asset_id, "type": "CryptoAsset", "properties": {"symbol": symbol, "source": "Binance Spot live trades"}})
        signals = bar.get("rule_signals", [])
        self._nodes[event_id] = {
            "id": event_id,
            "type": "MarketEvent",
            "properties": {
                "symbol": symbol,
                "timestamp": bar["bucket_end_exchange_time"],
                "price": bar["close"],
                "base_volume": bar["base_volume"],
                "trade_count": bar["trade_count"],
                "rule_signals": signals,
                "suspicious": bool(signals),
                "source": "Binance Spot five-second aggregate",
            },
        }
        self._graph_order.append(event_id)
        self._edges.append({"source": event_id, "target": asset_id, "type": "OBSERVED_ON", "properties": {}})
        previous = self._last_graph_event.get(symbol)
        if previous and previous in self._nodes:
            self._edges.append({"source": previous, "target": event_id, "type": "NEXT", "properties": {"interval_seconds": 5}})
        self._last_graph_event[symbol] = event_id
        while len(self._graph_order) > MAX_GRAPH_EVENTS:
            old = self._graph_order.popleft()
            self._nodes.pop(old, None)
            self._edges = deque((edge for edge in self._edges if edge["source"] != old and edge["target"] != old), maxlen=MAX_GRAPH_EVENTS * 2)
            for key, value in list(self._last_graph_event.items()):
                if value == old:
                    self._last_graph_event.pop(key, None)

    def _connection_payload_locked(self) -> dict:
        return {
            "connection_status": self._state,
            "exchange": "Binance Spot",
            "watchlist": list(self._symbols),
            "connected_at": self._connected_at,
            "last_error": self._last_error,
            "reconnect_attempts": self._reconnect_attempts,
            "last_received_at": self._last_received_at,
            "last_exchange_event_at": self._last_exchange_event_at,
            "last_exchange_trade_at": self._last_trade_at,
            "processed_events": self._event_count,
            "invalid_messages": self._invalid_messages,
        }

    def _publish_locked(self, event_type: str, payload: dict) -> None:
        self._sequence += 1
        event = {"id": self._sequence, "type": event_type, **payload}
        self._sse_events.append(event)
        self._condition.notify_all()

    def wait_events(self, after_id: int, timeout: float = 15.0) -> list[dict]:
        with self._condition:
            if not any(event["id"] > after_id for event in self._sse_events):
                self._condition.wait(timeout=max(0.05, timeout))
            return [dict(event) for event in self._sse_events if event["id"] > after_id]

    def status(self) -> dict:
        with self._lock:
            state = self._connection_payload_locked()
            bars_per_symbol = {symbol: len(self._bars.get(symbol, ())) for symbol in self._symbols}
            rule_state = "warming_up" if not any(count for count in bars_per_symbol.values()) else "active"
            state.update({
                "session_id": self._session_id,
                "session_started_by_user": self._desired_running,
                "stream_sequence": self._sequence,
                "bars_per_symbol": bars_per_symbol,
                "rule_detector": {
                    "status": rule_state,
                    "price_move_threshold_pct": PRICE_MOVE_THRESHOLD * 100,
                    "volume_spike_multiple": VOLUME_SPIKE_MULTIPLIER,
                    "volume_baseline_bars": VOLUME_BASELINE_BARS,
                    "note": "Only observed trade data is used. Five-second bars are closed when a trade from a later bucket arrives; empty buckets are not fabricated.",
                },
                "xgboost_live": {"status": "unavailable_live_schema", "reason": MODEL_UNAVAILABLE_REASON},
            })
            return state

    def snapshot(self) -> dict:
        with self._lock:
            state = self._connection_payload_locked()
            state.update({
                "session_id": self._session_id,
                "session_started_by_user": self._desired_running,
                "stream_sequence": self._sequence,
                "latest_prices": {symbol: {k: v for k, v in item.items() if not k.startswith("_")} for symbol, item in self._latest_prices.items()},
                "bars": {symbol: list(items) for symbol, items in self._bars.items()},
                "current_bars": {symbol: {k: v for k, v in bar.items()} for symbol, bar in self._current_bars.items()},
                "trades": list(reversed(self._trades))[:200],
                "alerts": list(reversed(self._alerts))[:100],
                "graph": {"nodes": list(self._nodes.values()), "edges": list(self._edges)},
                "rule_detector": self.status()["rule_detector"],
                "xgboost_live": {"status": "unavailable_live_schema", "reason": MODEL_UNAVAILABLE_REASON},
                "captured_at": utc_now(),
            })
            return state

    def graph(self, limit: int = 300) -> dict:
        bounded = max(1, min(int(limit), 1_200))
        with self._lock:
            nodes = list(self._nodes.values())
            node_ids = {node["id"] for node in nodes}
            edges = [edge for edge in self._edges if edge["source"] in node_ids and edge["target"] in node_ids]
            event_count = len(self._graph_order)
        return {"nodes": nodes[-bounded:], "edges": edges[-bounded * 2:], "stats": {
            "total_nodes": len(nodes), "total_edges": len(edges), "market_bars": event_count,
            "assets": sum(node["type"] == "CryptoAsset" for node in nodes),
            "rule_flagged_bars": sum(bool(node["properties"].get("suspicious")) for node in nodes if node["type"] == "MarketEvent"),
            "scope": "live_session_only",
        }}


live_market = LiveMarketManager()
