"""InfluxDB v2 metrics persistence and query helpers."""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone

_ALLOWED_RANGE = {"15m", "1h", "6h", "24h", "7d", "30d"}


class InfluxMetricsStore:
    def __init__(self, url=None, token=None, org=None, bucket=None):
        self.url = (url if url is not None else os.getenv("INFLUXDB_URL", "")).strip().rstrip("/")
        self.token = token if token is not None else os.getenv("INFLUXDB_TOKEN", "")
        self.org = org if org is not None else os.getenv("INFLUXDB_ORG", "")
        self.bucket = bucket if bucket is not None else os.getenv("INFLUXDB_BUCKET", "")
        self._client = None
        self._last_error = None

    @property
    def configured(self):
        return bool(self.url and self.token and self.org and self.bucket)

    def _get_client(self):
        if not self.configured:
            raise RuntimeError("InfluxDB is not configured; set INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG and INFLUXDB_BUCKET")
        if self._client is None:
            from influxdb_client import InfluxDBClient
            self._client = InfluxDBClient(url=self.url, token=self.token, org=self.org,
                                          timeout=3000, enable_gzip=True)
        return self._client

    def write_batch_metrics(self, metrics: dict):
        from influxdb_client import Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS
        point = Point("cryptosentinel_pipeline")
        for tag in ("source", "run_id", "batch_id"):
            value = metrics.get(tag)
            if value is not None and str(value):
                point = point.tag(tag, str(value)[:160])
        fields = ("input_records", "processed_events", "flagged_predictions",
                  "normal_predictions", "invalid_records", "processing_errors",
                  "processing_latency_ms", "dropped_records")
        wrote = False
        for field in fields:
            value = metrics.get(field)
            if isinstance(value, bool):
                value = int(value)
            if isinstance(value, (int, float)) and value == value and abs(value) != float("inf"):
                point = point.field(field, value)
                wrote = True
        if not wrote:
            return False
        timestamp = _parse_time(metrics.get("timestamp"))
        point = point.time(timestamp, WritePrecision.NS)
        client = self._get_client()
        client.write_api(write_options=SYNCHRONOUS).write(bucket=self.bucket, org=self.org, record=point)
        self._last_error = None
        return True

    def write_service_health(self, statuses: dict, checked_at=None):
        from influxdb_client import Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS
        points = []
        now = _parse_time(checked_at)
        for name, item in statuses.items():
            if name == "influxdb":
                continue
            status = str(item.get("status", "unknown"))
            up = int(status in {"connected", "healthy", "processing", "running"})
            configured = int(bool(item.get("configured")))
            points.append(Point("cryptosentinel_services").tag("service", name)
                          .tag("status", status).field("up", up)
                          .field("configured", configured).time(now, WritePrecision.NS))
        if not points or not self.configured:
            return 0
        client = self._get_client()
        client.write_api(write_options=SYNCHRONOUS).write(bucket=self.bucket, org=self.org, record=points)
        self._last_error = None
        return len(points)

    def query_recent(self, limit=500, time_range="1h"):
        if time_range not in _ALLOWED_RANGE:
            raise ValueError(f"time_range must be one of {', '.join(sorted(_ALLOWED_RANGE))}")
        limit = max(1, min(int(limit), 2000))
        flux = f'''from(bucket: "{_flux_escape(self.bucket)}")
  |> range(start: -{time_range})
  |> filter(fn: (r) => r._measurement == "cryptosentinel_pipeline" or r._measurement == "cryptosentinel_services")
  |> keep(columns: ["_time", "_measurement", "_field", "_value", "source", "run_id", "batch_id", "service", "status"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})'''
        client = self._get_client()
        tables = client.query_api().query(query=flux, org=self.org)
        records = []
        for table in tables:
            for record in table.records:
                records.append({
                    "time": _parse_time(record.get_time()).isoformat().replace("+00:00", "Z"),
                    "measurement": record.get_measurement(), "field": record.get_field(),
                    "value": record.get_value(), "source": record.values.get("source"),
                    "run_id": record.values.get("run_id"), "batch_id": record.values.get("batch_id"),
                    "service": record.values.get("service"), "status": record.values.get("status"),
                })
        self._last_error = None
        return {"available": True, "source": "influxdb", "bucket": self.bucket,
                "time_range": time_range, "records": records, "returned": len(records)}

    def health(self):
        if not self.configured:
            return {"status": "not_configured", "configured": False,
                    "detail": "Set INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET and INFLUXDB_TOKEN to enable time-series metrics."}
        try:
            result = self._get_client().health()
            status = getattr(result, "status", "unknown")
            if str(status).lower() != "pass":
                raise RuntimeError(f"InfluxDB health status: {status}")
            return {"status": "connected", "configured": True, "url": self.url,
                    "organization": self.org, "bucket": self.bucket}
        except Exception as exc:
            self._last_error = str(exc)[:240]
            return {"status": "unavailable", "configured": True, "error": self._last_error}

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None


def _parse_time(value):
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _flux_escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')
