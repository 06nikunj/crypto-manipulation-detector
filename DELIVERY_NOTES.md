# CryptoSentinel integrated-stack delivery notes

## What changed

- Added a pinned local Docker Compose stack: Apache Kafka 4.1.1 (single-node KRaft), Spark Structured Streaming 4.2.0 with the matching Scala 2.13 Kafka connector, Neo4j Community 5.26.31, InfluxDB 2.7.12, Grafana 12.4.2, and the existing Flask API/dashboard.
- Added local-only credentials through `.env.example`, explicit resource/port limits, service health checks, named data/checkpoint volumes, topic initialization, and an ownership initializer for the Kafka data volume. The Compose ports bind to host loopback. `scripts/run_pipeline.sh` validates that credential placeholders have been replaced before starting the stack.
- Wired both actual validated Binance Spot trade messages and managed historical CSV replay to Kafka. The original in-process benchmark replay remains separately available.
- Added a Spark worker that validates versioned JSON, dead-letters invalid records, builds event-time five-second Binance bars and transparent live rules, and applies the existing saved scaler/XGBoost only to the historical benchmark schema.
- Added an API Kafka consumer, manual offset commit after handling, Neo4j idempotent observed-market-event writes/reads, Influx batch/service metrics, a provisioned Grafana datasource/dashboard, and System Health status/replay controls.
- Preserved the live-only alert boundary: no historical XGBoost score is claimed for raw live trades. The graph describes observed market events/assets/groups; it is not a wallet or transaction graph.
- Rewrote `README.md` with the end-to-end architecture/data flow, exact local setup, service access, API routes, tests, persisted-data behavior and limitations. Added a current-state follow-up to the baseline `TECH_STACK_AUDIT.md`.

## Key files

- `docker-compose.yml`, `.env.example`, `scripts/run_pipeline.sh` — local stack definition, secret template and safe startup helper.
- `backend/api/Dockerfile`, `backend/spark/Dockerfile`, `backend/neo4j/Dockerfile`, `backend/influxdb/Dockerfile`, `backend/grafana/Dockerfile` — pinned service images/builds. The obsolete zero-byte `frontend/Dockerfile` was removed because the dashboard is served by Flask.
- `backend/integrations/service_pipeline.py` — acknowledged Kafka publishing, CSV replay controller, output consumer and health writer.
- `backend/streaming/spark_processor.py` — separate historical and live processing branches.
- `backend/integrations/neo4j_store.py`, `backend/integrations/influx_store.py` — parameterized/idempotent graph writes and bounded time-series queries.
- `backend/api/app.py`, `backend/live/market_stream.py` — compatible Flask APIs and the direct public Binance feed.
- `backend/grafana/provisioning/`, `backend/grafana/dashboards/` — automatically provisioned datasource and real-measurement dashboard.
- `frontend/index.html`, `frontend/system_health.js`, and existing frontend clients/styles — integration status, replay controls and preserved dashboard views.
- `requirements.txt`, `tests/test_integrations.py` — required Python clients and offline integration/model tests.

### Exact file manifest for this stack pass

- Root/deployment: `.env.example`, `docker-compose.yml`, `requirements.txt`, `scripts/run_pipeline.sh`, `README.md`, `DELIVERY_NOTES.md`, `TECH_STACK_AUDIT.md`.
- Flask and processing: `backend/api/Dockerfile`, `backend/api/app.py`, `backend/integrations/__init__.py`, `backend/integrations/service_pipeline.py`, `backend/integrations/neo4j_store.py`, `backend/integrations/influx_store.py`, `backend/streaming/spark_processor.py`, and `backend/live/market_stream.py`.
- Containers and observability: `backend/spark/Dockerfile`, `backend/neo4j/Dockerfile`, `backend/influxdb/Dockerfile`, `backend/grafana/Dockerfile`, `backend/grafana/provisioning/datasources/influxdb.yml`, `backend/grafana/provisioning/dashboards/provider.yml`, and `backend/grafana/dashboards/cryptosentinel-overview.json`.
- Dashboard and tests: `frontend/index.html`, `frontend/system_health.js`, and `tests/test_integrations.py`. The previously added live-market client, styles, Kafka bridge, Binance source notes and unit tests remain in the project package.
- Removed: the empty, unused `frontend/Dockerfile`; the actual Flask/dashboard image is `backend/api/Dockerfile`.

## How to run on macOS

1. Install Docker Desktop with Compose v2 and allocate at least 8 GB RAM to Docker (12 GB recommended).
2. From the project root run:

   ```bash
   cp .env.example .env
   openssl rand -hex 24   # generate local password material
   openssl rand -hex 32   # generate an Influx token
   $EDITOR .env           # enter unique credentials; save privately
   ./scripts/run_pipeline.sh
   ```

3. Wait for service health with `docker compose ps`; open the dashboard at `http://127.0.0.1:8000`. Start historical Kafka replay from **System Health**, or connect desired pairs in the live dashboard. Grafana is at port 3000, Neo4j Browser at 7474, and InfluxDB UI at 8086. See `README.md` for topic/record inspection, shutdown, data retention and complete API details.
4. Normal shutdown is `docker compose down`. `docker compose down -v` is destructive and erases local databases and the Spark checkpoint volume.

## Verification performed in this environment

| Check | Result |
|---|---|
| Unit/integration tests | **17 passed**, including actual saved-model/scaler inference and output-to-store adapter routing. |
| Python, JavaScript and shell syntax | **Passed** for backend/scripts/tests, all three dashboard clients, and the Compose startup helper. |
| Flask test-client API smoke checks | **Passed** for home, health, pipeline/replay/live status and metrics. Unsupported metrics range returned 400; Kafka replay correctly refused without a connected broker. |
| Compose structural validation | **Passed** against the Compose Specification schema. Grafana dashboard JSON/provisioning YAML parse, and every service build context/Dockerfile exists. |
| README Mermaid diagram | Rendered and visually inspected. |
| Docker image build and service startup | **Not run**: this sandbox has no Docker, Docker Compose CLI, or Podman. |
| Kafka→Spark→Neo4j/Influx/Grafana end-to-end/read-back | **Not run** for the same reason. |
| Live Binance-to-Kafka delivery in this pass | **Not run**; requires the started Docker stack and upstream public WebSocket access. |

The Compose checks validate structure, not actual image builds or runtime behavior. The first Docker Desktop run is still the required acceptance test. Keep the stack local: the Flask API has no production authentication/TLS, and the included broker/databases use a single-machine research configuration.

No failing tests remain in the final verification run. The first run found one metrics-range validation regression when InfluxDB was unconfigured; it was fixed before the full suite passed.
