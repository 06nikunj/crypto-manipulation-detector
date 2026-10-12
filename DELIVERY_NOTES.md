# CryptoSentinel live-market update — delivery notes

## What changed

- Added a **real Binance Spot public `@trade` WebSocket client** with a user-started, read-only connection, validated/deduplicated watchlist (maximum 20 pairs), combined subscription, reconnect backoff, ping-frame handling and bounded session state.
- Added server-side live snapshots, SSE updates, recent trade and alert endpoints, and a live graph endpoint. Live session state is separate from historical CSV replay state.
- Added exchange-trade/event timestamps and local receipt times, actual tick tables, latest prices, closed five-second OHLC/volume bars and bar-derived graph relationships. Empty buckets are not fabricated.
- Added transparent rule warnings for a ≥1% five-second close-to-close move or base volume ≥3× the median of the preceding 20 observed non-empty bars. Warm-up is shown for the volume rule. Warnings are not proof or a validated manipulation classification.
- Preserved the historical benchmark, XGBoost evaluation, replay, and benchmark events/alerts/graph in clearly secondary research views. The live path does **not** apply the saved XGBoost model because the benchmark feature schema is not validated for raw Binance trades.
- Replaced the legacy Kafka REST-polling producer with an **optional standalone WebSocket-to-Kafka bridge**. Kafka is not required or started by the Flask dashboard. No Spark job is started.
- Added public-feed source notes and updated project run, attribution and scope documentation.

## Key files

- `backend/live/market_stream.py` — public stream manager, input validation, actual trade parsing, observed bars, rule alerts, bounded in-memory graph/session state and SSE event sequencing.
- `backend/api/app.py` — isolated `/api/live/*` endpoints; historical replay endpoints remain intact.
- `frontend/live_market.js` — live UI, SSE client, observed-price chart, live events/alerts/graph and operational health.
- `frontend/index.html` — eight live-first navigation areas and separate historical benchmark pages.
- `frontend/obsidian.css` — responsive styles for live controls, event/alert tables, graph and health.
- `backend/kafka/binance_producer.py` — optional public WebSocket trade bridge to Kafka.
- `tests/test_live_market_stream.py` — offline unit tests for trade validation, aggregation, alert rules and symbol validation.
- `docs/binance_market_stream_sources.md`, `README.md` — upstream reference and operating guide.

## Validation performed

- Python compilation: live manager, Flask app, Kafka bridge and test module — passed.
- `node --check frontend/live_market.js` — passed.
- HTML ID audit — no duplicate IDs; eight primary navigation items and distinct live/benchmark page targets.
- Offline test suite — 6 tests passed. Coverage includes real message schema, malformed/unsubscribed trade rejection, OHLCV aggregation from trades, no fabricated buckets, price/volume rule behavior and symbol validation.
- End-to-end Flask API test — API health/status responded, live connection control succeeded, and the Python WebSocket client connected to Binance Spot. It received actual BTCUSDT, ETHUSDT and BNBUSDT trade messages (five during the short validation window); observed fields included prices, quantities, exchange timestamps and local receipt times.
- Live XGBoost state explicitly returned `unavailable_live_schema`; no live ML score was asserted.
- This validation confirms the tested environment and moment only. Public network access, exchange availability, local regulations and upstream terms can change.

## Setup

Python 3.12 is recommended. See `README.md` for macOS/Linux and Windows commands. The public feed is server-to-server and read-only; it requires internet access but no Binance account key. The dashboard stays disconnected until the user presses **Connect**.

The project ZIP is supplied with its root folder, requirements, code and documentation intact. The temporary hosted preview is an ephemeral sandbox URL and may stop when the workspace ends.

## Remaining boundaries

- The rule thresholds are demonstration heuristics; they are not calibrated or validated for market-manipulation diagnosis.
- XGBoost model metrics and predictions remain bound to the bundled historical benchmark; there is no live XGBoost inference.
- A live market-bar graph is not a wallet, order-book or transaction graph.
- Live state, bar history, alerts and graph data are bounded and process-local; they reset when the stream session/application restarts. No persistence or historical archive is claimed.
- Kafka is optional and external; the dashboard does not monitor a separate Kafka process. Spark, Neo4j and Grafana are not started by the dashboard.
- Binance stream semantics, connection limits and applicable terms should be rechecked by the operator. Links and dated facts are recorded in `docs/binance_market_stream_sources.md`.
