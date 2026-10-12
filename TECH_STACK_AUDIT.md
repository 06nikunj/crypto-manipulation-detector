# CryptoSentinel — Full Technology Stack Audit

**Project:** Real-Time Cryptocurrency Market Manipulation Detection Using Big Data Graph Streaming Analytics
**Repository provided:** [06nikunj/crypto-manipulation-detector](https://github.com/06nikunj/crypto-manipulation-detector)
**Audit date:** 2026-10-12
**Baseline scope:** Findings below describe the initial read-only audit, before the implementation follow-up at the end of this file. Read the follow-up for the current repository status.
**Scope:** Read-only inspection and reversible local verification. No application source, configuration, datasets, or model artifacts were intentionally changed. The historical replay started for the end-to-end test was stopped afterward; the already-running live read-only Binance session was left untouched.

## Executive summary

**The repository does not currently implement the proposed Kafka → Spark → XGBoost → Neo4j → InfluxDB → Flask/Grafana pipeline.** Several technologies have filenames or prototype code, but the service definitions are empty and the components are not connected into that flow.

Two useful application paths do work:

1. **Live path:** Binance Spot public trade WebSocket → Python validation and observed five-second bars → simple live price/volume rules → process-local graph/session → Flask REST/SSE → custom HTML/CSS/JavaScript dashboard. During this audit, the API reported a connected live session, 182,681 processed trades, zero invalid messages, and a fresh exchange timestamp. The stream uses no exchange credentials and is not sent through Kafka or Spark.
2. **Historical benchmark path:** labeled CSV → chronological Python replay queue → Pandas feature extraction → saved XGBoost/scaler (or a historical fallback) → bounded Python graph → Flask endpoints and dashboard. An actual replay processed 451 CSV rows with the model loaded, zero pipeline errors, and 451 graph writes. This is a local Python threaded pipeline, **not Apache Spark**.

The frontend/API and the historical model path are demonstrable. Kafka producer/consumer source code exists, but no broker is supplied or running and Kafka is not the dashboard’s processing path. Spark is not implemented as a Spark job. Neo4j writing is only an unused optional function/research code, and Grafana/InfluxDB lack operational configuration and data flows.

## Status definitions

- **Working:** exercised successfully in this environment for the stated scope.
- **Partially Working:** useful code/path works, but only with limited scope or without the full target integration.
- **Present but Not Integrated:** related source exists, but it is not connected to the running application or end-to-end path.
- **Not Implemented:** no operational implementation/configuration was found.
- **Not Tested:** no local service/runtime/credentials were available for a meaningful service test. This is a test result, not evidence that the service could never work elsewhere.

## Technology audit

| Technology | Required project files | Actual implementation | Verification result | Missing work |
|---|---|---|---|---|
| **Apache Kafka** | `backend/kafka/binance_producer.py`, `csv_producer.py`, `test_consumer.py`, `requirements.txt` | Producer code exists for public Binance trades and historical CSV replay; a diagnostic consumer reads a topic. The default topic is `market_trades`. The optional live bridge publishes raw trades. The Flask dashboard does not consume from Kafka; historical detection uses a local Python queue. | **Present but Not Integrated.** `kafka-python` is installed in this sandbox environment. No broker was available: ports 9092/9093 were closed and Docker was unavailable. Producer-to-consumer/broker delivery was **not tested**. | Provide and configure a broker; define topic/retention/serialization; make a real consumer (e.g. Spark) read the topic; route processed results to the application. The CSV producer’s dry-run path only logs; it does not provide Kafka delivery evidence. |
| **Apache Spark** | `backend/spark/Dockerfile`, `backend/spark/src/ingestion.py`, `preprocessing.py`, `anomaly_detection.py`, `graph_builder.py`, `requirements.txt` | The directory name and module docstrings say “Spark,” but the runtime code inspected uses Pandas/NumPy/scikit-learn, Python `threading`, and `queue.Queue`. No `SparkSession`, `pyspark`, `readStream`, `writeStream`, Structured Streaming job, Kafka source, or `spark-submit` path was found. | **Not Implemented.** No `spark-submit`/`pyspark` command was available and the Python `pyspark` package was absent. A separate Spark job was **not run**. The similarly named Python pipeline did run. | Implement an actual Spark application, specify supported Spark/Kafka versions and checkpointing, connect it to an input source, and document a service/job start path. |
| **Neo4j** | `backend/neo4j/graph_builder.py`, `backend/neo4j/Dockerfile`, optional function `backend/spark/src/graph_builder.py::try_neo4j_write` | `backend/neo4j/graph_builder.py` has connection and Cypher/GAT research functions. `try_neo4j_write` can attempt a single event/asset write, but a repository search found no call site in the application. The dashboard graph is maintained in bounded Python dictionaries/lists in `backend/spark/src/graph_builder.py` and `backend/live/market_stream.py`. No graph read-back route uses Neo4j. The Neo4j Dockerfile is empty; source uses hard-coded localhost/default credentials. | **Present but Not Integrated.** Bolt/HTTP ports 7687/7474 were closed and the Python `neo4j` package was absent. Database connectivity, writes, and reads were **not tested**. In-memory graph API calls returned HTTP 200. | Configure secrets through environment variables, provide a database service, add the driver dependency, call the writer from the event path, implement Neo4j-backed queries/API behavior, and verify persistence/read-back. Replace default credentials before any demonstration. |
| **Grafana** | `backend/grafana/Dockerfile`, `frontend/src/dashboard_config.py` | The Dockerfile is empty. `dashboard_config.py` is a Python dictionary describing panel ideas; it is not Grafana provisioning YAML or an imported Grafana dashboard, and it is not loaded by the Flask app. No data source/dashboard provisioning files were found. | **Not Implemented.** Port 3000 was closed, Docker was unavailable, and no Grafana service/dashboard could be opened or tested. | Supply a Grafana service, provision a real data source and dashboard JSON/YAML, connect it to a functioning metrics backend, and test panel queries. |
| **InfluxDB** | `backend/influxdb/Dockerfile`, a datasource name in `frontend/src/dashboard_config.py` | The Dockerfile is empty. No Influx client dependency, client usage, bucket/organization/token configuration, measurement schema, write path, or query path was found. The string `InfluxDB` in the mock dashboard dictionary is not a connection. | **Not Implemented.** Port 8086 was closed, the `influxdb_client` Python package was absent, and no write/query test could run. | Define credentials and bucket/org, add an actual metrics write/read module and retention policy, connect Grafana to it, and test persisted writes and queries. |
| **Flask API** | `backend/api/app.py`, `requirements.txt` | Serves the HTML dashboard and static assets; exposes live status/start/stop/snapshot/events/alerts/graph and SSE routes; exposes historical replay controls, in-memory events/alerts/graph, dataset and stored evaluation routes. | **Working** for the exercised local app. Health, dataset, sample, evaluation, live, benchmark and graph endpoints returned HTTP 200. Live SSE returned `text/event-stream` and an initial `snapshot`. During replay, event and graph endpoints returned actual processed application state. | Persistence, auth/deployment hardening and integration to the missing services remain future work; current results are local and in memory. |
| **XGBoost** | `scripts/train_models.py`, `backend/spark/src/classification.py`, `backend/spark/src/preprocessing.py`, `backend/spark/src/anomaly_detection.py`, `models/xgboost_detector.joblib`, `models/scaler.joblib`, `models/eval_metrics.json` | Training and inference share the same 12 `FEATURE_COLS` list. Historical replay computes the feature dict, orders it by those columns, scales it, and calls `predict_proba`. The saved model is loaded by the historical pipeline. The live Binance path explicitly disables this model because live trade fields do not reproduce/validate the benchmark feature schema (including `rush_order_ratio`). | **Working** for historical benchmark inference, **not for live trades**. Direct inference on an actual CSV-derived feature row produced a finite two-class probability. The replay ran 451 rows, `model_loaded=true`, `errors=0`. The stored evaluation JSON was served, but its metrics are artifacts—not a fresh full test-set run. | Do not connect the historical model to live raw trades without a validated live feature-generation/data-evaluation process. Separately verify training/test reproducibility if presenting evaluation scores. |
| **Frontend** | `frontend/index.html`, `frontend/live_market.js`, `frontend/obsidian.js`, `frontend/obsidian.css` | Custom browser dashboard with Overview, Live Markets, Events, Alerts, Graph Analysis, Model Evaluation, Dataset & Benchmark, and System Health. Live controls use REST; live updates use `EventSource` against Flask SSE. Historical views use benchmark API routes. It is not Grafana. | **Working** for served assets and backend integration. `/`, both JavaScript bundles and CSS returned HTTP 200. The API/SSE endpoint tests passed. Browser page-by-page interaction was not exhaustively repeated as part of this audit. | No frontend dependency on Kafka/Spark/Neo4j/Grafana/Influx exists. If Grafana is a formal deliverable, build and verify it separately rather than treating the current UI as Grafana. |
| **Live market ingestion** | `backend/live/market_stream.py`, Flask routes in `backend/api/app.py`, `frontend/live_market.js`, `docs/binance_market_stream_sources.md` | Direct Binance Spot combined `@trade` WebSocket, configurable `BINANCE_SYMBOLS`, schema/symbol/value validation, reconnect loop, five-second bars built from received trades, transparent price/volume rules, bounded process-local state and SSE. This is separate from the labeled historical CSV replay. | **Working** at audit time. `/api/live/status` reported `connected`, watchlist BTCUSDT/ETHUSDT/BNBUSDT, 182,681 processed messages, zero invalid messages, a fresh last-trade timestamp, and five reconnect attempts over the session. SSE returned an initial snapshot. This verifies the direct WebSocket path in this environment, not Kafka/Spark. | No durable storage or live XGBoost. Add production connection monitoring/rate-limit handling and independent live-model validation only if required by project scope. |

## What the repository actually contains

### Infrastructure and startup audit

- `docker-compose.yml` is **0 bytes**; it defines no services.
- `backend/grafana/Dockerfile`, `backend/influxdb/Dockerfile`, `backend/neo4j/Dockerfile`, `backend/spark/Dockerfile`, and `frontend/Dockerfile` are **0 bytes**.
- `scripts/run_pipeline.sh` is **0 bytes**; it starts nothing.
- No `.env.example`, Kafka broker/topic config, Spark config, Grafana provisioning, Influx initialization, or service-specific environment examples were found.
- `requirements.txt` includes Flask, WebSocket, Pandas/NumPy/scikit-learn/XGBoost and the Kafka Python client. It does not declare PySpark, Neo4j driver, InfluxDB client, Grafana, or any server runtime. Grafana, InfluxDB, Kafka and Neo4j are services, not ordinary Python package dependencies.
- The names `backend/spark`, `backend/grafana` and “Neo4j-compatible” in filenames/docstrings do not by themselves represent running services.
- Docker CLI/daemon and `spark-submit` were unavailable in the audit environment. Broker/database/dashboard service ports were closed at the time checked: Kafka 9092/9093, Neo4j Bolt/HTTP 7687/7474, Grafana 3000, InfluxDB 8086; Spark UI/worker examples 4040/7077 were also closed.

### Kafka producer/consumer specifics

There is meaningful Kafka client code, but no broker configuration or broker service. `csv_producer.py` defaults to `localhost:9092` and `market_trades`; it can fall back to a logging-only dry-run when Kafka is unavailable. `test_consumer.py` consumes a topic for diagnostics. `binance_producer.py` can forward public live trades to Kafka when an external broker is available. None of these files is a Kafka broker, and no application consumer connects Kafka messages to the historical detection pipeline or dashboard.

### Graph/database specifics

The dashboard graphs are Python in-memory structures and are served by Flask. Historical graph endpoints return the current process's bounded graph; live graph endpoints return the current live session's market-bar graph. `try_neo4j_write` is defined but never called in the application source. Neo4j/GAT research code is separate and not a persistent backing store for the dashboard. The available dataset has market event/group information, not wallet addresses; it cannot support claims that the current graph detects wallet-to-wallet transactions.

### Metrics and dashboard specifics

The only model evaluation metrics served by `/api/evaluation` come from the existing `models/eval_metrics.json`. There is no operational metrics write/query path to InfluxDB. `frontend/src/dashboard_config.py` describes four possible panel objects, including datasource names `InfluxDB` and `Neo4j`; no actual Grafana dashboard import/provisioning or datasource configuration consumes this dictionary.

## End-to-end data-flow audit

| Proposed connection | Evidence in repository | Finding |
|---|---|---|
| Market feed → Kafka | `backend/live/market_stream.py`; optional listener used in `backend/kafka/binance_producer.py` | **Optional standalone path exists**, but the dashboard’s working live path is direct WebSocket → Flask process memory. No broker was running; delivery was not verified. |
| Kafka → Spark | No Kafka source/consumer integration in a Spark job; no Spark APIs found | **Missing.** The diagnostic Kafka consumer prints messages; it does not feed the detector. |
| Spark → feature engineering | `backend/spark/src/preprocessing.py` uses Pandas/NumPy | Feature engineering exists in Python, but **not Spark**. |
| Feature engineering → XGBoost | `preprocessing.py`, `anomaly_detection.py`, `classification.py` | **Connected for historical replay**. The same 12 names/order are used by training and inference; tested on CSV-derived input. **Not connected for raw live trades.** |
| XGBoost → graph analytics | `anomaly_detection.py` sends enriched historical records to `ENRICHED_QUEUE`; Python `graph_builder.py` consumes it | **Connected to an in-memory historical event graph**, not Neo4j. The live path separately builds its own in-memory market-bar graph. |
| Graph → Neo4j | Optional `try_neo4j_write` function; no caller/service config | **Missing from runtime path.** No write/read-back or persistent database test. |
| Detection/graph → InfluxDB | No client, schema, writer, or query code | **Missing.** |
| InfluxDB → Grafana | Python dictionary only; no provisioning/data source | **Missing.** |
| Flask API → custom frontend | `backend/api/app.py`, frontend HTML/JS | **Working.** HTTP routes and assets returned successfully; actual live status, dataset and evaluation data were returned. |
| Flask API → Grafana | No Grafana/API/data-source wiring | **Missing.** |

**Defensible architecture today:**

```text
Live: Binance public WebSocket → Python live manager/rules/in-memory graph → Flask REST/SSE → custom dashboard

Historical: labeled CSV → Python chronological replay queue → Pandas features → XGBoost/scaler → Python in-memory graph → Flask → custom dashboard

Optional, unverified: Binance WebSocket → Kafka producer → external Kafka topic
```

The complete chain “market source → Kafka → Spark → XGBoost → Neo4j → metrics store → Flask/Grafana” is **not present**.

## Verification record

### Passed

- **Unit tests:** `python -m unittest discover -s tests -v` — 6 live-market unit tests passed (trade validation, aggregation, rules and symbol validation).
- **Python syntax:** `python -m compileall -q backend scripts tests` — completed successfully.
- **JavaScript syntax:** `node --check frontend/live_market.js` and `node --check frontend/obsidian.js` — both succeeded.
- **Shell syntax:** `bash -n scripts/run_pipeline.sh` — syntactically valid, but the file is empty and therefore performs no startup.
- **Flask HTTP smoke checks:** `/api/health`, `/api/live/status`, `/api/live/snapshot`, `/api/live/events`, `/api/live/alerts`, `/api/live/graph`, `/api/dataset`, `/api/dataset/sample`, `/api/evaluation`, `/api/stream/status`, `/api/events`, and `/api/graph/stats` all returned HTTP 200 in the running local app.
- **Live SSE:** `/api/live/stream` returned HTTP 200 with `text/event-stream` and an initial `snapshot` event.
- **Historical model inference:** loaded the bundled joblib model and scaler, derived 12 columns in the training order from actual CSV rows, and obtained finite class probabilities for a representative row.
- **Historical API end-to-end:** started the actual CSV replay through `POST /api/stream/start`, observed 451 rows processed, `model_loaded=true`, zero pipeline errors, and 451 graph writes; stopped replay through `POST /api/stream/stop` afterward.
- **Frontend asset serving:** `/`, `/live_market.js`, `/obsidian.js`, and `/obsidian.css` all returned HTTP 200.
- **Live feed:** active status and fresh exchange/receipt timestamps were observed, as detailed above.

### Not tested because the required service/runtime was unavailable

- **Kafka broker publication/consumption:** no broker at the configured local ports; no Docker available. Client-side producer and consumer source were inspected, but no end-to-end broker test was performed.
- **Spark job:** no Spark runtime or Spark job implementation.
- **Neo4j persistence:** no database service and no Neo4j driver installed.
- **InfluxDB write/query:** no service, client library, schema, or credentials/config.
- **Grafana dashboard/data source:** no service, dashboard provisioning, or data-source configuration.
- **Full browser-driven page/control regression:** asset serving and API/SSE integration were checked, but all pages were not manually clicked during this audit.

**No test above establishes service availability outside this sandbox.** In particular, the stored evaluation JSON is not a test run, and a successful Python unit test of a client is not a Kafka/Spark/Neo4j/Grafana/Influx service test.

## Exact Mac commands for the current project

### Start the currently implemented Flask app

From Terminal, enter the extracted project directory (replace the path if needed):

```bash
cd /path/to/crypto-manipulation-detector-main
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT PORT=5001 python -m backend.api.app
```

Open `http://127.0.0.1:5001` and select **Connect** on Live Markets to start the direct public Binance connection. The exchange feed needs working outbound WebSocket connectivity. No API key is required. To verify the Flask app and live path from a second Terminal:

```bash
curl -fsS http://127.0.0.1:5001/api/health
curl -fsS http://127.0.0.1:5001/api/live/status
curl -N --max-time 5 http://127.0.0.1:5001/api/live/stream
```

Start the separate historical CSV/model path from the dashboard’s benchmark replay controls. To exercise it from Terminal instead, use the actual API controls:

```bash
curl -fsS -X POST http://127.0.0.1:5001/api/stream/start \
  -H 'Content-Type: application/json' -d '{"speed":100}'
curl -fsS http://127.0.0.1:5001/api/stream/status
curl -fsS -X POST http://127.0.0.1:5001/api/stream/stop \
  -H 'Content-Type: application/json' -d '{}'
```

The historical replay is local Python; this command does not start Spark or Kafka.

### Kafka client commands when a Kafka broker is separately installed and running

The project does **not** start the broker. In one Terminal, with a broker available at `localhost:9092`, a bounded historical test send is:

```bash
cd /path/to/crypto-manipulation-detector-main
.venv/bin/python backend/kafka/csv_producer.py \
  --broker localhost:9092 --topic market_trades --rows 5 --speed 1000
```

In another Terminal, inspect up to five messages:

```bash
cd /path/to/crypto-manipulation-detector-main
.venv/bin/python backend/kafka/test_consumer.py \
  --broker localhost:9092 --topic market_trades --limit 5 --timeout 30
```

For the optional actual Binance-to-Kafka publisher, once the broker and topic are ready:

```bash
cd /path/to/crypto-manipulation-detector-main
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=market_trades \
  .venv/bin/python backend/kafka/binance_producer.py
```

This forwards public raw trades; it does **not** start Spark or send events into the dashboard detector. The repository has no Kafka broker startup command or verified topic-creation policy, so a successful producer/consumer run must be demonstrated on the user's broker before claiming Kafka works end to end.

### Spark, Neo4j, Grafana and InfluxDB

**There is currently no valid project command to start these services.** The Compose file and their Dockerfiles are empty; Spark code is not a Spark job; Neo4j is not wired to the application; Grafana has no dashboard/data-source provisioning; InfluxDB has no client/schema/write/query path. `docker compose up` cannot start this intended stack from this repository. This audit deliberately does not provide a made-up “successful” service startup command. Install/configure and implement these services first, then add their exact Mac startup/verification commands to the project documentation.

## What is already complete

1. A local Flask app that serves the custom frontend and working live/historical API surfaces.
2. A direct public Binance Spot trade stream that was connected and delivering current trade events during this audit.
3. Live observed five-second aggregation, transparent price/volume rule warnings, and a live in-memory market-bar graph.
4. A separate labeled-CSV chronological replay path, feature extraction, saved XGBoost/scaler inference, alert/event state, and in-memory graph generation.
5. Six passing live unit tests and a usable local dashboard UI.
6. Optional Kafka producer and diagnostic-consumer code that can be tested when a broker is provided.

## What is incomplete or broken relative to the proposed Big Data stack

1. No Kafka broker/service definition and no consumer joining Kafka to the detector.
2. No Apache Spark runtime/job; current “stream processing” is Python threads, queues, and Pandas.
3. No operational Neo4j integration or database-backed graph reads/writes; hard-coded fallback credentials are not suitable for use.
4. No InfluxDB storage, schema, credentials, writes, or queries.
5. No operational Grafana service, dashboard file, provisioned data source, or metrics backend.
6. All Compose/Dockerfile/start-pipeline placeholders inspected are empty.
7. Historical XGBoost is not a live-trade detector. This is a documented and technically preferable boundary until the live schema and performance are validated.
8. No durable event/metric persistence: API graphs, counters, alerts and live session state are process-local/bounded.

## Prioritized repair plan (proposal only; no implementation performed)

### Priority 0 — Decide the target architecture and semantics

Before implementing infrastructure, agree whether faculty requires a genuinely distributed pipeline or whether the current working direct-WebSocket/Python prototype is sufficient. If the intended chain must be demonstrated, decide which path owns live ingestion (direct Flask versus Kafka producer), what messages/feature contract cross each boundary, where model inference runs, and which data is stored in Neo4j versus InfluxDB. Keep historical benchmark evaluation separate from unvalidated live-market warnings.

### Priority 1 — Make the services reproducible

- **`docker-compose.yml`** — define pinned Kafka, Spark, Neo4j, InfluxDB, Grafana, and application services, health checks, networks, volumes, dependencies and startup order. Currently 0 bytes.
- **`.env.example` (new)** — document non-secret variable names for broker URLs/topics, Neo4j connection, Influx org/bucket/token, and Grafana configuration; keep real secrets out of source control.
- **`backend/spark/Dockerfile`**, **`backend/neo4j/Dockerfile`**, **`backend/influxdb/Dockerfile`**, **`backend/grafana/Dockerfile`**, **`frontend/Dockerfile`** — either supply meaningful service images or remove placeholder Dockerfiles from the claimed deployment architecture. Currently all are 0 bytes.
- **`scripts/run_pipeline.sh`** — implement or replace with documented Compose startup, health checks, and clean shutdown. Currently 0 bytes.
- **`README.md`** — document pinned prerequisites, environment setup, service startup, topic creation, and per-service verification. Avoid calling components operational until tested.

### Priority 2 — Wire Kafka to actual processing

- **`backend/kafka/binance_producer.py`** — make broker/topic settings and message contract explicit; report delivery errors and verify topic readiness.
- **`backend/kafka/csv_producer.py`** — keep historical replay clearly labeled and ensure dry-run results cannot be mistaken for sent messages.
- **`backend/kafka/test_consumer.py`** — retain as a diagnostic consumer or replace it with the application processing consumer.
- **New `backend/spark/src/kafka_streaming.py` (or equivalent)** — implement a real Spark Structured Streaming Kafka source, schema validation, checkpointed processing, and controlled shutdown. Specify compatible PySpark/Spark Kafka connector versions in the service image/configuration.
- **`requirements.txt`** — update only for libraries actually used by the selected runtime. A Spark Docker image may manage Spark and its Kafka connector separately; do not confuse a Python package install with a running Spark cluster.

### Priority 3 — Validate inference contract before any live-model claim

- **`backend/spark/src/preprocessing.py`** — define a versioned, documented feature contract and specify which fields can be derived from the chosen real feed.
- **`backend/spark/src/anomaly_detection.py`** and **`backend/spark/src/classification.py`** — keep ordering/scaling consistent and add training-versus-serving parity tests. Do not synthesize unavailable `rush_orders` or other benchmark-only inputs to force live XGBoost compatibility.
- **`tests/` (new historical and integration tests)** — test representative labeled data, feature values, model/scaler dimensions, threshold behavior, and a separately labeled live-schema validation set before enabling model scoring on live input.

### Priority 4 — Persist graphs and metrics and provision Grafana

- **`backend/spark/src/graph_builder.py`** and **`backend/neo4j/graph_builder.py`** — move connection settings to environment variables, inject a driver, call the Neo4j writer from the selected processing path, use safe upserts and indexes, and implement/test database-backed reads. Do not represent the current in-memory graph as Neo4j.
- **New `backend/metrics/influx_writer.py` and query module** — define measurement/tag/field schema, timestamps, batching, retries, retention, and authorized query methods for InfluxDB.
- **`requirements.txt`** — add a pinned Neo4j/Influx client only once the integration code requires it.
- **New `backend/grafana/provisioning/datasources/…` and `backend/grafana/dashboards/…` files** — provision InfluxDB as an actual datasource and include importable dashboard JSON backed by real stored metrics.
- **`frontend/src/dashboard_config.py`** — either turn it into a documented generator/export or remove its ambiguity; it is not a provisioned Grafana dashboard today.

### Priority 5 — Demonstrate end-to-end and document evidence

- Add integration tests/scripts for Kafka produce/consume, Spark query progress, Neo4j write/read-back, Influx write/query, Grafana datasource/dashboard loading, and API/frontend data flow.
- Add readiness/health checks that distinguish “service reachable,” “pipeline processing,” and “data persisted.”
- Record screenshots/logs and exact commands only after tests succeed. Keep a checklist of untested external prerequisites.

## University project review checklist

- [ ] Explain that live Binance ingestion and historical benchmark replay are separate paths.
- [ ] Show live status with an exchange timestamp and actual received trades; explain reconnects and public-feed limitations.
- [ ] Explain live alerts as rule warnings—not confirmed manipulation and not XGBoost predictions.
- [ ] Demonstrate historical replay separately and show the historical feature names/order, saved model/scaler status, and source dataset attribution.
- [ ] State whether each displayed metric is a stored artifact, a current process counter, or a freshly computed result.
- [ ] Show the graph data source and schema; call it in-memory unless a working Neo4j write/read-back has been demonstrated.
- [ ] If claiming Kafka, show a real broker, topic, producer delivery, consumer receipt, and the downstream processor reading the same messages.
- [ ] If claiming Spark, show an actual Spark application/query (not merely a folder name), runtime status, and input/output progress.
- [ ] If claiming Neo4j, show a persisted node/relationship and query it back from the database.
- [ ] If claiming InfluxDB, show a real measurement write and query result.
- [ ] If claiming Grafana, show the live dashboard, provisioned datasource, and panels querying the functioning metrics backend.
- [ ] Run the documented startup on the presentation Mac from a clean checkout/environment; verify health endpoints and service dependencies.
- [ ] Disclose absent services and limitations; do not call a code placeholder, in-memory structure, or stored benchmark score a running distributed service.

---

**Audit disposition:** Core direct-feed and historical demo paths were verified. The proposed Kafka/Spark/Neo4j/Grafana/InfluxDB architecture remains incomplete. This report proposes changes but does not implement them; obtain the project's preferred target architecture before any infrastructure repair.


## Implementation follow-up — 2026-10-12

The baseline audit above is a historical snapshot, not the current state. Since that snapshot, the repository has been updated with a local Docker Compose path for Kafka/KRaft, Spark Structured Streaming, Neo4j, InfluxDB, Grafana, and the existing Flask dashboard. The original in-process historical replay and direct Binance WebSocket interface remain distinct alternatives/inputs; neither is mislabeled as the Kafka/Spark path.

The source now includes: a schema-validated Spark worker with separate live-bar rule and historical 12-feature XGBoost branches; an acknowledged Kafka publisher and managed benchmark replay controller; an API Kafka consumer that writes processed events to Neo4j and batch/service measurements to InfluxDB; database-backed graph reads with an identified local fallback; provisioned Grafana datasource/dashboard; a health/replay view and environment template; and a startup helper that rejects an unconfigured or placeholder `.env`. The unused empty `frontend/Dockerfile` was removed. The current architecture, setup and semantics are documented in `README.md`.

### Verification after implementation

| Check | Result |
|---|---|
| Unit/integration tests | **17 passed**; includes inference using the repository's real saved model and scaler, feature dimensionality, Kafka record provenance, live-rule separation, store routing and missing-service behavior. |
| Python compilation | **Passed** for `backend`, `scripts` and `tests`. |
| JavaScript syntax | **Passed** for `obsidian.js`, `live_market.js` and `system_health.js`. |
| Startup helper syntax | **Passed** with `bash -n scripts/run_pipeline.sh`. |
| Flask test-client smoke checks | **Passed** for dashboard, health, pipeline status, replay status, live status and metrics; unsupported metrics ranges return 400; replay correctly refuses to start without Kafka. |
| Compose specification/schema | **Passed** against the current Compose Specification schema; Grafana provisioning YAML and dashboard JSON parse; all build contexts and Dockerfiles exist. |
| README Mermaid architecture | **Rendered successfully** and visually inspected. |
| Docker image build / Compose service startup | **Not run**: Docker Engine/Compose is unavailable in this sandbox. Therefore no live Kafka→Spark→Neo4j/Influx→Grafana end-to-end or read-back is claimed. |
| Live Binance→Kafka delivery | **Not run during this implementation validation**; it requires the actual Docker stack and an available upstream public WebSocket. |

The first automated test run exposed a metrics validation edge case when InfluxDB was unconfigured. It was fixed, and the full suite passed afterward. Compose validation also caught malformed Grafana dashboard JSON and an incorrect KRaft cluster-ID environment name; both were corrected before final validation. Static/schema checks do not prove a container image will build or a service will start. The next required acceptance step is to copy `.env.example` to `.env`, use unique local credentials, and follow the macOS startup and event-flow steps in `README.md` on a Docker Desktop machine.
