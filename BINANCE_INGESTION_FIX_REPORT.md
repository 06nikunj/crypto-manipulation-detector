# CryptoSentinel Binance ingestion fix report

**Date:** 2026-10-12
**Source:** `crypto-manipulation-detector-main.zip` uploaded in this conversation. The ZIP contains no `.git` directory, so there is no Git history/status to report. Work was performed in a fresh extraction of that uploaded version, not the older workspace copy.

## Targeted changes

- `backend/live/market_stream.py` now defaults to `wss://data-stream.binance.vision`, retains the `BINANCE_WS_BASE` override (including `wss://stream.binance.com:9443`), passes `certifi.where()` and `ssl.CERT_REQUIRED` to `WebSocketApp.run_forever()`, and reports WebSocket exception type plus message in `last_error`. Existing subscriptions, parsing, reconnect behavior, listeners/Kafka publication, Spark boundary, APIs and SSE behavior were not redesigned.
- Root `requirements.txt` pins Certifi; the API image installs the system `ca-certificates` bundle as well.
- `docker-compose.yml` passes the configurable `BINANCE_WS_BASE` into the API container. `.env.example` documents the new working default and existing-endpoint override.
- `tests/test_live_market_stream.py` covers URL construction/default and legacy override, required Certifi verification, SSL options passed to `run_forever()`, and connecting → connected → reconnecting status/error reporting.

No SSL-verification bypass (`CERT_NONE`) was introduced. The live XGBoost boundary remains `unavailable_live_schema`; raw live trades are not scored by that benchmark model.

## Tests and verification performed

- Commands run in the fresh uploaded extraction:

  ```bash
  python3 -m venv .venv
  .venv/bin/python -m pip install -r requirements.txt
  .venv/bin/python -m unittest discover -s tests -v
  .venv/bin/python -m compileall -q backend tests
  node --check frontend/obsidian.js
  node --check frontend/live_market.js
  node --check frontend/system_health.js
  bash -n scripts/run_pipeline.sh
  .venv/bin/pip check
  ```

- `.venv/bin/python -m unittest discover -s tests -v` — **20 tests passed**.
- The status-transition test intentionally triggers a fake handshake error; it verifies the API state becomes `reconnecting` and retains the error details.
- Started the uploaded Flask app natively on port **8011** (`PORT=8011 .venv/bin/python -m backend.api.app`); port 8000 was already occupied by another resident sandbox app, so it was left untouched. A Python `urllib.request` check sent `POST /api/live/start` with `{"symbols":["BTCUSDT","ETHUSDT","BNBUSDT"]}`, then polled `GET /api/live/status` and `GET /api/live/snapshot` for up to 35 seconds.
- Native API result at 2026-10-12 16:03:24 UTC: `connection_status=connected`, `processed_events=4,794`, `last_error=null`; the snapshot contained actual recent trades and latest prices/timestamps for **BTCUSDT, ETHUSDT and BNBUSDT**. This run used the uploaded app's default URL and Certifi TLS configuration, not simulated or replayed data.
- Opened the uploaded dashboard at `http://127.0.0.1:8011/` in the sandbox browser. It rendered “Connected to live trades,” displayed current BTC/ETH prices and a live trade count (4,502 at capture), with BTCUSDT, ETHUSDT and BNBUSDT selected.
- In the same native run, Kafka and Spark status were both `not_configured` because Docker services were not running.
- Runtime availability check: `for c in docker docker-compose podman; do command -v "$c" || true; done; docker info --format '{{.ServerVersion}}'` — Docker was not installed; no Compose or Podman executable was found.

## Docker verification boundary

`docker` is not installed in this sandbox (`docker: command not found`); Docker Compose and Podman are unavailable as well. Therefore **the API-container image was not rebuilt, and live trades inside Docker, Kafka delivery, Spark receipt, and container-to-dashboard end-to-end flow were not verified here**. Native API and browser-dashboard evidence is not a substitute for that requested container check.

## Commands to run on the Mac

From the uploaded project root, while the existing Compose stack is running:

```bash
docker compose up -d --build --no-deps api
docker compose ps
```

This rebuilds/recreates only the API service; Kafka, Spark and databases are left running. If the stack is not running yet, use the project's normal startup procedure first.

Start the real live session and check its state:

```bash
curl -fsS -X POST http://127.0.0.1:8000/api/live/start \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["BTCUSDT","ETHUSDT","BNBUSDT"]}'
curl -fsS http://127.0.0.1:8000/api/live/status
curl -fsS http://127.0.0.1:8000/api/live/snapshot
```

For a machine-checked wait (up to 60 seconds) for all three symbols:

```bash
python3 - <<'PY'
import json, time, urllib.request
base = "http://127.0.0.1:8000"
def get(path):
    with urllib.request.urlopen(base + path, timeout=10) as r:
        return json.load(r)
for _ in range(60):
    status = get("/api/live/status")
    snapshot = get("/api/live/snapshot")
    prices = snapshot.get("latest_prices", {})
    if status.get("connection_status") == "connected" and all(s in prices for s in ("BTCUSDT", "ETHUSDT", "BNBUSDT")):
        print(json.dumps({"connection_status": status["connection_status"],
                          "processed_events": status["processed_events"],
                          "last_exchange_event_at": status["last_exchange_event_at"],
                          "symbols": {s: prices[s] for s in ("BTCUSDT", "ETHUSDT", "BNBUSDT")}}, indent=2))
        break
    time.sleep(1)
else:
    print(json.dumps(status, indent=2))
    raise SystemExit("Did not receive actual trades for all requested pairs")
PY
```

Then open **http://127.0.0.1:8000** and confirm the live banner, prices and received-trade count update. Check Kafka/Spark separately rather than inferring them from the direct feed:

```bash
curl -fsS http://127.0.0.1:8000/api/pipeline/status
```

If the API reports Kafka as connected, inspect actual topic records and Spark service health using the project's README procedures. Direct Binance API connectivity alone does not prove Kafka or Spark ingestion.
