"""Neo4j persistence for observed market events and detector outputs.

Only observed market-event relationships are stored. This project has no wallet
or transaction-address data, so this module never creates such relationships.
"""
from __future__ import annotations

import os
from typing import Any


def _plain(value: Any):
    if hasattr(value, "iso_format"):
        return value.iso_format()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


class Neo4jGraphStore:
    def __init__(self, uri=None, user=None, password=None, database=None):
        self.uri = (uri if uri is not None else os.getenv("NEO4J_URI", "")).strip()
        self.user = user if user is not None else os.getenv("NEO4J_USER", "neo4j")
        self.password = password if password is not None else os.getenv("NEO4J_PASSWORD", "")
        self.database = database or os.getenv("NEO4J_DATABASE", "neo4j")
        self._driver = None
        self._last_error = None
        self._schema_ready = False

    @property
    def configured(self):
        return bool(self.uri and self.password)

    def _get_driver(self):
        if not self.configured:
            raise RuntimeError("Neo4j is not configured; set NEO4J_URI, NEO4J_USER and NEO4J_PASSWORD")
        if self._driver is None:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(
                self.uri, auth=(self.user, self.password),
                connection_timeout=3.0, connection_acquisition_timeout=3.0,
                max_connection_lifetime=300,
            )
        return self._driver

    def ensure_schema(self):
        if self._schema_ready:
            return
        driver = self._get_driver()
        statements = [
            "CREATE CONSTRAINT cs_asset_symbol IF NOT EXISTS FOR (n:CryptoAsset) REQUIRE n.symbol IS UNIQUE",
            "CREATE CONSTRAINT cs_event_record_id IF NOT EXISTS FOR (n:MarketEvent) REQUIRE n.record_id IS UNIQUE",
            "CREATE CONSTRAINT cs_alert_id IF NOT EXISTS FOR (n:ManipulationAlert) REQUIRE n.alert_id IS UNIQUE",
            "CREATE CONSTRAINT cs_group_code IF NOT EXISTS FOR (n:PumpGroup) REQUIRE n.group_code IS UNIQUE",
            "CREATE INDEX cs_event_time IF NOT EXISTS FOR (n:MarketEvent) ON (n.event_time)",
            "CREATE INDEX cs_event_suspicious IF NOT EXISTS FOR (n:MarketEvent) ON (n.suspicious)",
        ]
        with driver.session(database=self.database) as session:
            for statement in statements:
                session.run(statement).consume()
        self._schema_ready = True
        self._last_error = None

    def write_events(self, events: list[dict]):
        rows = []
        for item in events:
            if item.get("record_type") not in (None, "processed_event"):
                continue
            symbol = str(item.get("symbol", "")).strip().upper()
            record_id = str(item.get("record_id") or item.get("event_id") or "").strip()
            if not symbol or not record_id:
                continue
            timestamp = item.get("event_timestamp") or item.get("timestamp")
            if not timestamp:
                continue
            label = str(item.get("label") or "")
            flagged = bool(item.get("is_alert")) or label == "PUMP_AND_DUMP" or item.get("model_prediction") == 1
            rows.append({
                "record_id": record_id,
                "event_id": str(item.get("event_id") or record_id),
                "source": str(item.get("source") or item.get("source_type") or "unknown"),
                "symbol": symbol,
                "event_time": str(timestamp),
                "price": _numeric(item.get("price")),
                "volume": _numeric(item.get("volume")),
                "quote_volume": _numeric(item.get("quote_volume")),
                "label": label,
                "suspicious": flagged,
                "risk_score": _numeric(item.get("risk_score")),
                "ground_truth": item.get("ground_truth"),
                "group": str(item.get("group") or "").strip(),
                "detection_method": str(item.get("detection_method") or ""),
                "signals": list(item.get("signals") or []),
                "alert_id": str(item.get("alert_id") or (f"alert:{record_id}" if flagged else "")),
                "explanation": str(item.get("explanation") or ""),
            })
        if not rows:
            return 0
        driver = self._get_driver()
        self.ensure_schema()
        base_query = """
        UNWIND $rows AS row
        MERGE (asset:CryptoAsset {symbol: row.symbol})
          ON CREATE SET asset.graph_id = 'asset_' + row.symbol,
                        asset.source = 'observed exchange market data'
        MERGE (event:MarketEvent {record_id: row.record_id})
          ON CREATE SET event.graph_id = 'event_' + row.record_id
        SET event.event_id = row.event_id,
            event.source = row.source,
            event.symbol = row.symbol,
            event.event_time = datetime(row.event_time),
            event.timestamp = row.event_time,
            event.price = row.price,
            event.volume = row.volume,
            event.quote_volume = row.quote_volume,
            event.label = row.label,
            event.suspicious = row.suspicious,
            event.risk_score = row.risk_score,
            event.ground_truth = row.ground_truth,
            event.detection_method = row.detection_method,
            event.rule_signals = row.signals,
            event.explanation = row.explanation
        MERGE (event)-[:OBSERVED_ON]->(asset)
        RETURN count(event) AS written
        """
        group_query = """
        UNWIND $rows AS row
        WITH row WHERE row.group <> ''
        MATCH (event:MarketEvent {record_id: row.record_id})
        MERGE (grp:PumpGroup {group_code: row.group})
          ON CREATE SET grp.graph_id = 'group_' + row.group
        MERGE (event)-[:ORGANIZED_BY]->(grp)
        RETURN count(grp) AS linked
        """
        alert_query = """
        UNWIND $rows AS row
        WITH row WHERE row.suspicious = true
        MATCH (event:MarketEvent {record_id: row.record_id})
        MERGE (alert:ManipulationAlert {alert_id: row.alert_id})
        SET alert.graph_id = 'alert_' + row.alert_id,
            alert.event_id = row.event_id,
            alert.symbol = row.symbol,
            alert.detected_at = datetime(row.event_time),
            alert.source = row.source,
            alert.label = row.label,
            alert.detection_method = row.detection_method,
            alert.signals = row.signals,
            alert.risk_score = row.risk_score,
            alert.explanation = row.explanation
        MERGE (alert)-[:FLAGS]->(event)
        RETURN count(alert) AS written
        """
        with driver.session(database=self.database) as session:
            session.execute_write(lambda tx: tx.run(base_query, rows=rows).consume())
            group_rows = [r for r in rows if r["group"]]
            if group_rows:
                session.execute_write(lambda tx: tx.run(group_query, rows=group_rows).consume())
            alert_rows = [r for r in rows if r["suspicious"]]
            if alert_rows:
                session.execute_write(lambda tx: tx.run(alert_query, rows=alert_rows).consume())
        self._last_error = None
        return len(rows)

    def graph_data(self, limit=300):
        limit = max(1, min(int(limit), 1200))
        driver = self._get_driver()
        node_query = """
        MATCH (n)
        WHERE n:CryptoAsset OR n:MarketEvent OR n:PumpGroup OR n:ManipulationAlert
        RETURN labels(n) AS labels, properties(n) AS props
        ORDER BY coalesce(n.event_time, n.detected_at) DESC
        LIMIT $limit
        """
        edge_query = """
        MATCH (a)-[r]->(b)
        WHERE (a:CryptoAsset OR a:MarketEvent OR a:PumpGroup OR a:ManipulationAlert)
          AND (b:CryptoAsset OR b:MarketEvent OR b:PumpGroup OR b:ManipulationAlert)
        RETURN a.graph_id AS source, b.graph_id AS target, type(r) AS type
        LIMIT $limit
        """
        with driver.session(database=self.database) as session:
            nodes = [{"id": row["props"].get("graph_id", ""),
                      "type": row["labels"][0] if row["labels"] else "MarketEvent",
                      "properties": _plain({k: v for k, v in row["props"].items() if k != "graph_id"})}
                     for row in session.run(node_query, limit=limit)]
            edges = [{"source": row["source"], "target": row["target"], "type": row["type"]}
                     for row in session.run(edge_query, limit=limit * 3)]
            counts = session.run("""
                MATCH (n) WITH count(n) AS nodes
                MATCH ()-[r]->() WITH nodes, count(r) AS edges
                RETURN nodes, edges
            """).single()
            by_type = session.run("""
                MATCH (n) UNWIND labels(n) AS label
                RETURN label, count(*) AS count
            """).data()
            suspicious = session.run("MATCH (n:MarketEvent) WHERE n.suspicious = true RETURN count(n) AS n").single()["n"]
        return {"nodes": nodes, "edges": edges,
                "stats": {"total_nodes": counts["nodes"], "total_edges": counts["edges"],
                          "suspicious_events": suspicious,
                          "node_types": {row["label"]: row["count"] for row in by_type}},
                "data_source": "neo4j", "graph_semantics": "observed market-event relationships; no wallet or transaction graph"}

    def suspicious_events(self, limit=100):
        driver = self._get_driver()
        query = """
        MATCH (event:MarketEvent) WHERE event.suspicious = true
        RETURN properties(event) AS props ORDER BY event.event_time DESC LIMIT $limit
        """
        with driver.session(database=self.database) as session:
            result = []
            for row in session.run(query, limit=max(1, min(int(limit), 1000))):
                props = _plain(row["props"])
                graph_id = props.pop("graph_id", "")
                result.append({"id": graph_id, "type": "MarketEvent", "properties": props})
            return result

    def health(self):
        if not self.configured:
            return {"status": "not_configured", "configured": False,
                    "detail": "Set NEO4J_URI, NEO4J_USER and NEO4J_PASSWORD to enable graph persistence."}
        try:
            driver = self._get_driver()
            driver.verify_connectivity()
            return {"status": "connected", "configured": True, "uri": _safe_uri(self.uri)}
        except Exception as exc:
            self._last_error = str(exc)[:240]
            return {"status": "unavailable", "configured": True, "error": self._last_error}

    def close(self):
        if self._driver is not None:
            self._driver.close()
            self._driver = None
        self._schema_ready = False


def _numeric(value):
    try:
        number = float(value)
        return number if number == number and abs(number) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _safe_uri(uri):
    return uri.split("@", 1)[-1] if "@" in uri else uri
