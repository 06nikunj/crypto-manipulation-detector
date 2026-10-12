# CryptoSentinel — Live Market Monitor & Historical Benchmark

CryptoSentinel is a research prototype with two explicitly separate data paths:

1. **Live Markets:** a user-started, read-only connection to Binance Spot's public WebSocket trade streams. The dashboard shows received trades, locally recorded receipt times, observed five-second bars, and explainable price/volume rule warnings.
2. **Historical Benchmark:** the bundled SystemsLab–Sapienza labeled dataset, its saved XGBoost artifacts, and optional replay/evaluation views. Benchmark rows and scores are never presented as current exchange activity.

> **Not a trading system or manipulation verdict.** The live rule engine produces review cues, not proof of pump-and-dump activity. The saved XGBoost model is not applied to raw Binance trades because its benchmark-specific feature schema has not been validated for them.

## Live dashboard

The primary navigation is **Overview, Live Markets, Events, Alerts, Graph Analysis, Model Evaluation, Dataset & Benchmark, and System Health**. The historical replay, benchmark event/alert pages, and historical relationship graph remain available as secondary research tools from Dataset & Benchmark or the replay page.

The live connection starts only when **Connect** is pressed. The application server subscribes to a bounded list of public `@trade` streams and forwards actual messages to the browser over Server-Sent Events. No exchange API key, account access, trading permission, orders, or user data stream is used.

For each actual trade the dashboard can display the exchange trade ID, pair, price, base quantity, derived quote quantity, exchange event/trade timestamps, and local receipt timestamp. It aggregates received trades into **observed, non-empty five-second bars**. Bars close after a real trade arrives in a later bucket; the app does not manufacture empty bars or interpolate prices.

### Live rule warnings

The current demonstration rules operate on closed, observed five-second bars:

- A close-to-close price move of at least **1.0%**; or
- Base volume of at least **3× the median** of the preceding 20 observed, non-empty bars (the rule remains in warm-up until that baseline exists).

Warnings include source bar time, local detection time, the rule signal, and observed values. The threshold choices are heuristic, are not calibrated risk probabilities, and do not prove manipulation. There is **no live XGBoost score or prediction**. The saved model and its metrics remain confined to the historical benchmark views.

The live graph is bounded to the current process session. Green nodes represent subscribed pairs, small nodes represent observed market bars, red nodes crossed a displayed rule, and links represent observed pair/bar or consecutive-bar relationships. It contains **no wallets, addresses, ownership links, order-book depth, or transaction graph**.

## Requirements

- Python 3.12 is recommended for the bundled model artifacts and pinned requirements.
- Internet access is required for package installation and the optional Binance public WebSocket connection.
- The dashboard itself uses the local Flask app. Kafka, Spark, Neo4j, and Grafana are not required.

## Install and start

### macOS / Linux

```bash
cd "$HOME/Downloads/crypto-manipulation-detector-main"  # use the folder you extracted
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT PORT=5001 python -m backend.api.app
```

Open **http://127.0.0.1:5001**, select **Connect**, and wait for actual trade messages. Keep the Terminal window open. Stop the application with **Ctrl+C**. To use the default symbols, omit `BINANCE_SYMBOLS`; the default watchlist is BTCUSDT, ETHUSDT, and BNBUSDT.

### Windows PowerShell

```powershell
cd "$HOME\Downloads\crypto-manipulation-detector-main"  # use the folder you extracted
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:BINANCE_SYMBOLS = "BTCUSDT,ETHUSDT,BNBUSDT"
$env:PORT = "5001"
.\.venv\Scripts\python.exe -m backend.api.app
```

Open **http://127.0.0.1:5001** and press **Connect**. Stop with **Ctrl+C**. `BINANCE_SYMBOLS` accepts comma-separated uppercase Spot pair symbols; input is validated and deduplicated server-side, with a maximum of 20 pairs. Disconnecting or restarting clears the in-memory live session.

## Optional Kafka bridge

The dashboard's primary live path runs directly from the public WebSocket to its in-memory live manager; Kafka is optional. If you already have a Kafka broker available, the standalone bridge forwards the same actual public trade messages to the `market_trades` topic:

```bash
KAFKA_BROKERS=localhost:9092 KAFKA_TOPIC=market_trades \
  python3 backend/kafka/binance_producer.py
```

The `kafka-python` dependency is listed in `requirements.txt`. This script does not start a broker; set `BINANCE_SYMBOLS` as needed. No Spark streaming job is started by the dashboard.

## API reference

### Live session

- `GET /api/live/status` — API, connection, rule and model capability status.
- `POST /api/live/start` with `{"symbols":["BTCUSDT","ETHUSDT"]}` — start/replace the public stream.
- `POST /api/live/stop` — stop the live stream.
- `GET /api/live/snapshot` — bounded live trades, latest prices, bars, rule warnings and graph preview.
- `GET /api/live/events?limit=100` — recent raw validated trade messages.
- `GET /api/live/alerts?limit=50` — recent rule-based warnings.
- `GET /api/live/graph?limit=300` — bounded live market-bar graph.
- `GET /api/live/stream` — Server-Sent Events for `snapshot`, `status`, `trade`, `bar`, and `alert` updates.

### Historical benchmark

- `GET /api/health`
- `GET /api/stream/status`; `POST /api/stream/start`, `/pause`, `/resume`, `/stop`, `/speed`
- `GET /api/events`, `/api/events/summary`
- `GET /api/alerts`, `/api/alerts/episodes`; `POST /api/alerts/episodes/<episode-id>/acknowledge`
- `GET /api/graph/nodes`, `/suspicious`, `/stats`, `/neighbors/<node-id>`, `/asset/<symbol>`
- `GET /api/evaluation`, `/api/dataset`, and `/api/dataset/sample?limit=6`

Live counters, events, alerts and graph state are bounded and kept in process memory; they reset when a new live session or Flask process starts. Historical replay state remains separate.

## Benchmark, attribution and limitations

The bundled benchmark is the [SystemsLab–Sapienza Pump-and-Dump Dataset](https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset), associated with M. La Morgia, A. Mei, F. Sassi, and J. Stefa, “Pump and Dumps in the Bitcoin Era: Real Time Detection of Cryptocurrency Market Manipulations,” *ICCCN*, 2020, DOI: [10.1109/ICCCN49398.2020.9209660](https://doi.org/10.1109/ICCCN49398.2020.9209660). Source-file details and license attribution are in `data/README.md` and `data/pump_dump_dataset/LICENSE`.

Stored evaluation metrics describe the project's chronological historical test split only. They do not establish live-market accuracy, calibration, profitability, or generalization to other exchanges, assets, time periods, or manipulation strategies. The benchmark does not contain wallet addresses. It must not be interpreted as proof of wallet coordination, wash trading, spoofing, or current market activity. Launching the dashboard does not retrain or independently revalidate the model.

Binance interface references and implementation notes: [`docs/binance_market_stream_sources.md`](docs/binance_market_stream_sources.md), including the [official Spot WebSocket Streams documentation](https://developers.binance.com/en/docs/products/spot/web-sockets/market-data-streams). Review current exchange terms and applicable local rules before operating a public-data client.

## Project structure

```text
backend/api/app.py                     Flask routes and dashboard serving
backend/live/market_stream.py          Public trade client, five-second bars, rule detector and bounded live state
backend/kafka/binance_producer.py      Optional public WebSocket -> Kafka bridge
backend/spark/src/                       Historical replay, features, XGBoost and benchmark graph
frontend/index.html                     Live-first UI plus separate benchmark pages
frontend/live_market.js                 Live SSE client, charts, event/alert/graph renderers
frontend/obsidian.css                   Shared responsive design system
frontend/obsidian.js                    Historical benchmark UI renderers
models/                                  Saved model/scaler and benchmark metrics
data/                                    Bundled historical benchmark and attribution
scripts/                                 Training and evaluation utilities
tests/test_live_market_stream.py        Offline live-schema, bar and rule tests
```

## Verification

```bash
.venv/bin/python -m unittest discover -s tests -v
node --check frontend/live_market.js
curl http://127.0.0.1:5001/api/live/status
```

For upstream stream limits, payload definitions, and lifecycle details see [`docs/binance_market_stream_sources.md`](docs/binance_market_stream_sources.md).