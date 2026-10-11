# CryptoSentinel — Market Integrity Intelligence

CryptoSentinel is a research prototype for **historical pump-and-dump benchmark replay**. It replays the bundled SystemsLab–Sapienza labeled trade chunks in timestamp order, derives rolling features, scores each chunk with the saved XGBoost model, and presents events, alerts, graph relationships, and benchmark evaluation in a Flask-served dashboard.

> **Replay, not live detection.** The default app does not connect to a live exchange or ingest current market data. Replay speed controls only change how quickly historical rows are presented; they do not alter the data or prediction scores.

## What is included

- Seven dashboard areas: Overview, Stream, Events, Alerts, Graph, Evaluation, and Dataset.
- Flask JSON API and a responsive, same-origin HTML dashboard.
- Bundled `labeled_trades.csv`, `pump_events_clean.csv`, and `pump_telegram.csv` benchmark data.
- Saved XGBoost detector and scaler under `models/`, plus the stored chronological test metrics.
- An in-memory event graph. It describes market-event relationships; it does **not** represent wallet transactions.
- Kafka, Spark, Neo4j, and Grafana support files are parts of the broader project layout, but are not required to run the local dashboard demonstration.

## Architecture

```text
Bundled benchmark CSV
        │ timestamp-ordered replay (5-second observations; adjustable playback multiplier)
        ▼
Flask StreamController → rolling feature extraction → saved StandardScaler → XGBoost
                                                │                │
                                                ▼                ▼
                                      event/episode API     in-memory event graph
                                                │
                                                ▼
                                  CryptoSentinel dashboard
```

The replay preserves source chronology. For each observation, the inference path computes the 12 features used by the project model: `price_change_pct`, `price_std`, `volume`, `volume_change_pct`, `volume_std`, `n_trades`, `trade_freq`, `rush_order_ratio`, `buy_ratio`, `avg_trade_size`, `rolling_volatility`, and `anomaly_score`. Rolling windows include the current chunk and use the same sample-standard-deviation convention as training preprocessing.

Processed/normal/pump-prediction/error counters are distinct. Alert cards group flagged chunks by the dataset's event/window ID, while the graph assigns a separate node to each replayed chunk. Graph edges mean `BELONGS_TO` (event to asset), `NEXT` (consecutive observed chunks for that asset in a replay), and `ORGANIZED_BY` (a benchmark pump event's Telegram group code).

## Dataset attribution

The bundled benchmark is the [SystemsLab–Sapienza Pump-and-Dump Dataset](https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset), associated with:

> M. La Morgia, A. Mei, F. Sassi, and J. Stefa, “Pump and Dumps in the Bitcoin Era: Real Time Detection of Cryptocurrency Market Manipulations,” *2020 29th International Conference on Computer Communications and Networks (ICCCN)*, 2020, pp. 1–9. DOI: [10.1109/ICCCN49398.2020.9209660](https://doi.org/10.1109/ICCCN49398.2020.9209660).

The repository's `data/README.md` documents file schemas and original source attribution. Check the upstream repository's current terms before redistributing the benchmark. The provided `labeled_trades.csv` is the project-prepared labeled trade-chunk file; its labels and generated/engineered fields are benchmark data, not a feed of present-day exchange trades.

## Limitations and responsible interpretation

- This is a **historical benchmark replay**, not a market-connected live service, execution system, or trading recommendation.
- Evaluation scores in `models/eval_metrics.json` describe the project's stored chronological benchmark test split only. High benchmark precision/recall/F1/PR-AUC do **not** guarantee accuracy on live markets, other assets, exchanges, time periods, or manipulation strategies; they do not establish profitability.
- Risk scores are raw XGBoost output probabilities and are not externally calibrated. The default alert threshold is 0.50.
- The benchmark does not contain wallet addresses. The graph is an event/asset/group graph, not a wallet-to-wallet transaction graph; it cannot substantiate wallet coordination or wash trading.
- No live Telegram stream, exchange WebSocket, Kafka broker, Neo4j server, or Grafana instance is started by the dashboard command. The related project modules are optional/incomplete integrations for this local demonstration.
- The graph is an in-memory bounded view (up to 5,000 nodes and 10,000 edges), and the Events API presents the most recent 2,000 in-memory classified rows. Run counters and deduplicated episode totals are process-local and reset when the Flask process restarts.
- The supplied benchmark metrics are precomputed artifacts; launching the dashboard does not retrain or independently revalidate the model.

## Faculty walkthrough: what the dashboard actually does

1. **Start Replay** reads the bundled, prepared `labeled_trades.csv` in timestamp order and places historical 5-second observations on the local replay queue. The speed selector changes playback pacing only.
2. The serving preprocessor computes the model's 12 input features from the current observation and same-symbol rolling history. Ground-truth label fields are not selected as model features.
3. The saved scaler and XGBoost classifier return a **raw output score**. A score at or above the configured `0.50` threshold is a **model prediction**, not a confirmed manipulation event.
4. Every flagged 5-second chunk contributes to the flagged-chunk count. Chunks sharing a benchmark `event_id` are deduplicated into one episode card; its `flagged_chunks` value reports the number of flagged chunks in that window.
5. The graph writer records market-event, asset, and available benchmark group relationships in process memory. It has no wallet-address or transaction data.
6. Flask serves the actual in-process replay/graph state and the stored test metrics to the dashboard. The Evaluation tab reads `models/eval_metrics.json`; it does not silently rerun training.

**Counter scope:** `Events Processed` is the pipeline's cumulative number of dequeued chunks (including a chunk whose feature/model handling records an exception). `Normal Events` counts chunks predicted `NORMAL`; flagged chunks are counted separately as model predictions. Unique flagged episodes are distinct benchmark event/window IDs, not chunks. The Events explorer date/search/score filters operate on the bounded **most recent 2,000** in-memory event records; they do not query the entire CSV. The graph is also a bounded in-memory view. Replay counters, alert acknowledgments, and graph state reset when the Flask process restarts.

**For a short presentation:** open Overview and expand **How the data moves through CryptoSentinel**; start a replay; pause/resume it to show the controller; inspect a flagged prediction in Alerts and compare its benchmark label only in the expanded details; then show the confusion matrix and note that benchmark scores are not a live trading guarantee.

## Requirements

- macOS 13+ or a recent Linux distribution.
- Python 3.12 recommended for the bundled model artifacts and dependency set.
- Internet access for initial package installation. The benchmark CSVs and model artifacts are already bundled, so the dashboard does not need to download data at startup.

## Install and start on macOS

From Terminal, after extracting this project archive, run these commands from the project directory (replace the first path with the folder where you extracted it):

```bash
cd "$HOME/Downloads/crypto-manipulation-detector-main"
python3 --version                 # Python 3.12 recommended
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
PORT=5001 python -m backend.api.app
```

Keep that Terminal window open. Open **http://127.0.0.1:5001** in a browser. Use **Start Replay** or the Stream tab to start the historical replay; pause, resume, stop, and playback-speed controls are provided there. Stop the process with **Ctrl+C**. In a later Terminal session, restart with:

```bash
cd "$HOME/Downloads/crypto-manipulation-detector-main"
source .venv/bin/activate
PORT=5001 python -m backend.api.app
```

The dataset downloader and model trainer are not needed for the bundled demonstration. To intentionally retrain, use `python scripts/train_models.py`; this overwrites the model/scaler and evaluation artifacts, so keep a copy of the supplied artifacts if you need them.

## API reference

The local service exposes:

- `GET /api/health`
- `GET /api/stream/status`; `POST /api/stream/start`, `/pause`, `/resume`, `/stop`, `/speed`
- `GET /api/events` and `/api/events/summary`
- `GET /api/alerts`, `/api/alerts/episodes`; `POST /api/alerts/episodes/<episode-id>/acknowledge`
- `GET /api/graph/nodes`, `/suspicious`, `/stats`, `/neighbors/<node-id>`, `/asset/<symbol>`
- `GET /api/evaluation` and `/api/dataset`

## Project structure

```text
backend/api/app.py                    Flask routes and dashboard serving
backend/spark/src/ingestion.py        Historical CSV replay controller
backend/spark/src/preprocessing.py    Feature engineering and inference features
backend/spark/src/anomaly_detection.py XGBoost inference, counters, alert episodes
backend/spark/src/graph_builder.py     In-memory market-event relationship graph
frontend/index.html                    Seven-tab CryptoSentinel dashboard
data/pump_dump_dataset/               Bundled benchmark files
models/                                XGBoost, scaler, stored evaluation metrics
scripts/                               Training and evaluation utilities
```

## Quick verification

After starting the server, `curl http://127.0.0.1:5001/api/health` should return JSON. The dashboard refreshes the active view from the Flask API; events and alerts are not fabricated when the replay is stopped.
