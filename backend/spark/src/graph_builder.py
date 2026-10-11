"""
In-Memory Graph Analytics (Neo4j-compatible)
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Arnav (044) — Graph Analysis & Detection

Builds and queries a graph of market events derived from the
SystemsLab-Sapienza pump-and-dump dataset.

Consumes enriched events from the detection pipeline (anomaly_detection.py)
and constructs the graph. Provides graph queries to Aanya's dashboard (Member 4).

IMPORTANT: The dataset has no wallet addresses. The graph models MARKET EVENTS,
not wallet-to-wallet transactions. Relationships are:
  - BELONGS_TO: event → asset
  - NEXT: consecutive events for the same asset (time chain)
  - ORGANIZED_BY: pump events → Telegram group (group code from dataset)

Graph schema (event-centric):
  Nodes:
    CryptoAsset   {symbol}
    MarketEvent   {event_id, timestamp, symbol, price, volume, risk_score, label}
    PumpGroup     {group}  — group codes from pump_telegram.csv only

  Relationships:
    (MarketEvent)-[:BELONGS_TO]->(CryptoAsset)
    (MarketEvent)-[:NEXT]->(MarketEvent)   # consecutive 5-sec chunks, same symbol
    (MarketEvent)-[:ORGANIZED_BY]->(PumpGroup)  # pump events only

Neo4j persistence: attempted if Docker is running. Falls back to in-memory graph.
"""

import threading
import queue
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional
import json

# ── in-memory graph storage ────────────────────────────────────────────────────
_lock         = threading.Lock()
_nodes: dict[str, dict]       = {}   # node_id → {type, properties}
_edges: list[dict]             = []   # list of edge dicts
_adj: dict[str, list[str]]     = defaultdict(list)   # adjacency list (node_id → [node_id])

MAX_NODES = 5000
MAX_EDGES = 10000

# Track last event per symbol for NEXT relationships
_last_event_per_symbol: dict[str, str] = {}
_last_replay_id = None


def _add_node(node_id: str, node_type: str, props: dict):
    """Idempotent node upsert. Merges new properties into existing node."""
    with _lock:
        if node_id in _nodes:
            _nodes[node_id]["properties"].update(props)
        else:
            if len(_nodes) >= MAX_NODES:
                return   # backpressure — drop oldest nodes eventually
            _nodes[node_id] = {"type": node_type, "id": node_id, "properties": props}


def _add_edge(src_id: str, dst_id: str, rel_type: str, props: dict = None):
    """Add a directed edge (no dedup — caller ensures idempotency where needed)."""
    with _lock:
        edge = {
            "source"  : src_id,
            "target"  : dst_id,
            "type"    : rel_type,
            "properties": props or {},
        }
        _edges.append(edge)
        _adj[src_id].append(dst_id)
        if len(_edges) > MAX_EDGES:
            removed = _edges.pop(0)
            old_src = removed["source"]
            if old_src in _adj and removed["target"] in _adj[old_src]:
                _adj[old_src].remove(removed["target"])


def ingest_event(event: dict):
    """
    Add one enriched market event to the graph.

    Parameters
    ----------
    event : enriched event dict from anomaly_detection.ENRICHED_QUEUE
            Must have: event_id, symbol, event_timestamp, price, volume,
                       risk_score, label, group (optional)
    """
    episode_id = event.get("event_id", f"evt_{id(event)}")
    # Dataset event_id identifies a ~10-minute window, not an individual chunk.
    # A sequence-qualified node ID keeps each 5-second observation distinct.
    replay_id = event.get("replay_id", "session")
    event_id  = f"market_{replay_id}_{event.get('sequence', id(event))}"
    symbol    = event.get("symbol", "UNK")
    ts        = event.get("event_timestamp", "")
    risk      = event.get("risk_score", 0.0)
    label     = event.get("label", "NORMAL")
    group     = event.get("group", "unknown")
    price     = event.get("price", 0.0)
    volume    = event.get("volume", 0.0)
    n_trades  = event.get("n_trades", 0)

    # CryptoAsset node
    asset_id = f"asset_{symbol}"
    _add_node(asset_id, "CryptoAsset", {"symbol": symbol})

    # MarketEvent node
    _add_node(event_id, "MarketEvent", {
        "event_id"  : event_id,
        "episode_id": episode_id,
        "timestamp" : ts,
        "symbol"    : symbol,
        "price"     : round(price, 8),
        "volume"    : round(volume, 6),
        "n_trades"  : n_trades,
        "risk_score": round(risk, 4),
        "label"     : label,
        "suspicious": risk >= 0.5,
    })

    # BELONGS_TO → CryptoAsset
    _add_edge(event_id, asset_id, "BELONGS_TO")

    # NEXT → previous event for same symbol (chain consecutive events), but do
    # not draw a temporal edge across separate historical replay sessions.
    global _last_replay_id
    with _lock:
        if replay_id != _last_replay_id:
            _last_event_per_symbol.clear()
            _last_replay_id = replay_id
    with _lock:
        prev = _last_event_per_symbol.get(symbol)
    if prev and prev != event_id and prev in _nodes:
        _add_edge(prev, event_id, "NEXT", {"time_delta_seconds": 5})
    with _lock:
        _last_event_per_symbol[symbol] = event_id

    # PumpGroup node + ORGANIZED_BY relationship (pump events only)
    if label == "PUMP_AND_DUMP" and group not in ("unknown", None, ""):
        group_id = f"group_{group}"
        _add_node(group_id, "PumpGroup", {"group": group})
        _add_edge(event_id, group_id, "ORGANIZED_BY")


# ── query helpers ──────────────────────────────────────────────────────────────

def get_all_nodes(limit: int = 200) -> list[dict]:
    """Return up to `limit` nodes as dicts suitable for graph viz."""
    with _lock:
        nodes = list(_nodes.values())
    return nodes[:limit]


def get_all_edges(limit: int = 500) -> list[dict]:
    with _lock:
        edges = list(_edges)
    return edges[:limit]


def get_suspicious_events(limit: int = 100) -> list[dict]:
    """Return nodes of type MarketEvent with risk_score >= 0.5."""
    with _lock:
        nodes = list(_nodes.values())
    sus = [
        n for n in nodes
        if n["type"] == "MarketEvent"
        and n["properties"].get("suspicious", False)
    ]
    sus.sort(key=lambda n: n["properties"].get("risk_score", 0), reverse=True)
    return sus[:limit]


def get_neighbors(node_id: str, depth: int = 2) -> dict:
    """BFS from node_id up to `depth` hops. Returns subgraph dict."""
    visited_nodes: set[str] = set()
    frontier = {node_id}
    for _ in range(depth):
        new_frontier: set[str] = set()
        with _lock:
            for nid in frontier:
                for nbr in _adj.get(nid, []):
                    if nbr not in visited_nodes:
                        new_frontier.add(nbr)
        visited_nodes |= frontier
        frontier = new_frontier
    visited_nodes |= frontier

    with _lock:
        sub_nodes = [_nodes[nid] for nid in visited_nodes if nid in _nodes]
        sub_edges = [
            e for e in _edges
            if e["source"] in visited_nodes and e["target"] in visited_nodes
        ]
    return {"nodes": sub_nodes, "edges": sub_edges}


def get_graph_stats() -> dict:
    with _lock:
        n_nodes = len(_nodes)
        n_edges = len(_edges)
        type_counts: dict[str, int] = defaultdict(int)
        for n in _nodes.values():
            type_counts[n["type"]] += 1
        sus_count = sum(
            1 for n in _nodes.values()
            if n["type"] == "MarketEvent"
            and n["properties"].get("suspicious", False)
        )
    return {
        "total_nodes"      : n_nodes,
        "total_edges"      : n_edges,
        "node_types"       : dict(type_counts),
        "suspicious_events": sus_count,
    }


def get_asset_subgraph(symbol: str, limit: int = 50) -> dict:
    """Return all MarketEvent nodes and edges for a given symbol."""
    asset_id = f"asset_{symbol}"
    with _lock:
        # All events belonging to this asset
        event_ids = {
            e["source"] for e in _edges
            if e["target"] == asset_id and e["type"] == "BELONGS_TO"
        }
        event_ids.add(asset_id)
        sub_nodes = [_nodes[nid] for nid in event_ids if nid in _nodes]
        sub_edges = [
            e for e in _edges
            if e["source"] in event_ids and e["target"] in event_ids
        ]
    sub_nodes = sub_nodes[:limit]
    return {"nodes": sub_nodes, "edges": sub_edges}


# ── background writer thread ───────────────────────────────────────────────────

class GraphWriter:
    """
    Consumes enriched events from anomaly_detection.ENRICHED_QUEUE
    and writes them to the in-memory graph.
    """

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.written = 0

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="GraphWriter"
        )
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _loop(self):
        from backend.spark.src.anomaly_detection import ENRICHED_QUEUE
        while not self._stop.is_set():
            try:
                event = ENRICHED_QUEUE.get(timeout=0.5)
                ingest_event(event)
                self.written += 1
                ENRICHED_QUEUE.task_done()
            except queue.Empty:
                continue
            except Exception:
                continue

    def get_state(self) -> dict:
        return {"written": self.written}


graph_writer = GraphWriter()


# ── Neo4j optional writer ──────────────────────────────────────────────────────

def try_neo4j_write(event: dict,
                    uri: str = "bolt://localhost:7687",
                    user: str = "neo4j",
                    password: str = "password") -> bool:
    """
    Attempt to write an event node to a running Neo4j instance.
    Returns True on success, False if Neo4j is unavailable.
    Uses MERGE for idempotent writes.
    """
    try:
        from neo4j import GraphDatabase, exceptions as neo4j_exc
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            session.run(
                """
                MERGE (a:CryptoAsset {symbol: $symbol})
                MERGE (e:MarketEvent {event_id: $event_id})
                  ON CREATE SET
                    e.timestamp  = $timestamp,
                    e.symbol     = $symbol,
                    e.price      = $price,
                    e.volume     = $volume,
                    e.risk_score = $risk_score,
                    e.label      = $label
                MERGE (e)-[:BELONGS_TO]->(a)
                """,
                symbol    = event.get("symbol", "UNK"),
                event_id  = event.get("event_id", ""),
                timestamp = event.get("event_timestamp", ""),
                price     = float(event.get("price", 0)),
                volume    = float(event.get("volume", 0)),
                risk_score= float(event.get("risk_score", 0)),
                label     = event.get("label", "NORMAL"),
            )
        driver.close()
        return True
    except Exception:
        return False


# ── CLI smoke test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sample_events = [
        {"event_id": f"evt_{i}", "symbol": "BTC", "event_timestamp": "2019-03-01T10:00:00Z",
         "price": 0.001 * (1 + i * 0.1), "volume": 0.5 + i * 0.2,
         "n_trades": 20 + i * 5, "risk_score": 0.8 if i > 5 else 0.1,
         "label": "PUMP_AND_DUMP" if i > 5 else "NORMAL", "group": "TestGroup"}
        for i in range(10)
    ]
    for ev in sample_events:
        ingest_event(ev)

    stats = get_graph_stats()
    print("Graph stats:", json.dumps(stats, indent=2))
    sus = get_suspicious_events(5)
    print(f"Suspicious events: {len(sus)}")
    nbrs = get_neighbors("evt_6", depth=2)
    print(f"Neighbours of evt_6: {len(nbrs['nodes'])} nodes, {len(nbrs['edges'])} edges")
