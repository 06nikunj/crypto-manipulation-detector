"""
Spark/Pandas Feature Engineering & Preprocessing
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Sruthi (001) — Stream Processing & Feature Engineering

Computes time-window features from clean trade chunks produced by Member 1.
Features are derived ONLY from fields actually present in labeled_trades.csv:
  timestamp, symbol, price, volume, n_trades, rush_orders, buy_ratio, is_pump

These features are then passed to Member 3 (Arnav) for graph construction
and detection. Rolling statistics use a configurable window of N preceding chunks.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import joblib
import warnings

warnings.filterwarnings("ignore")

# ── constants ──────────────────────────────────────────────────────────────────
ROLLING_WINDOW = 5          # number of chunks for rolling statistics
CHUNK_SECONDS  = 5          # seconds per chunk (matches Sapienza default)
MODEL_DIR      = Path(__file__).parent.parent.parent.parent / "models"
SCALER_PATH    = MODEL_DIR / "scaler.joblib"

FEATURE_COLS = [
    "price_change_pct",
    "price_std",
    "volume",
    "volume_change_pct",
    "volume_std",
    "n_trades",
    "trade_freq",
    "rush_order_ratio",
    "buy_ratio",
    "avg_trade_size",
    "rolling_volatility",
    "anomaly_score",
]


# ── per-row computation ────────────────────────────────────────────────────────

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute features for each chunk row.

    Expects columns: timestamp, symbol, price, volume, n_trades, rush_orders, buy_ratio
    Returns DataFrame with FEATURE_COLS + original columns.
    """
    df = df.copy()
    df = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    results = []
    for symbol, grp in df.groupby("symbol", sort=False):
        grp = grp.sort_values("timestamp").reset_index(drop=True)

        # ── price features ─────────────────────────────────────────────────────
        grp["price_change_pct"] = grp["price"].pct_change().fillna(0).clip(-1, 10)
        grp["price_std"] = (
            grp["price"].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0)
        )

        # ── volume features ────────────────────────────────────────────────────
        grp["volume_change_pct"] = grp["volume"].pct_change().fillna(0).clip(-1, 50)
        grp["volume_std"] = (
            grp["volume"].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0)
        )

        # ── trade-count features ───────────────────────────────────────────────
        grp["trade_freq"] = grp["n_trades"] / CHUNK_SECONDS
        grp["avg_trade_size"] = np.where(
            grp["n_trades"] > 0, grp["volume"] / grp["n_trades"], 0.0
        )

        # ── rush order ratio ───────────────────────────────────────────────────
        grp["rush_order_ratio"] = np.where(
            grp["n_trades"] > 0,
            grp["rush_orders"] / grp["n_trades"],
            0.0,
        )

        # ── rolling volatility (std of % price changes) ────────────────────────
        grp["rolling_volatility"] = (
            grp["price_change_pct"].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(0)
        )

        # ── simple anomaly score: z-score of volume relative to rolling mean ───
        vol_roll_mean = grp["volume"].rolling(ROLLING_WINDOW, min_periods=1).mean()
        vol_roll_std  = grp["volume"].rolling(ROLLING_WINDOW, min_periods=1).std().fillna(1e-8)
        vol_roll_std  = vol_roll_std.replace(0, 1e-8)
        grp["anomaly_score"] = ((grp["volume"] - vol_roll_mean) / vol_roll_std).fillna(0)

        results.append(grp)

    out = pd.concat(results, ignore_index=True)

    # Guard against NaN / inf
    for col in FEATURE_COLS:
        if col in out.columns:
            out[col] = out[col].replace([np.inf, -np.inf], 0).fillna(0)

    return out


# ── scaler helpers ─────────────────────────────────────────────────────────────

def fit_scaler(X: np.ndarray) -> StandardScaler:
    """Fit and save a StandardScaler on training data."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    scaler = StandardScaler()
    scaler.fit(X)
    joblib.dump(scaler, SCALER_PATH)
    return scaler


def load_scaler() -> StandardScaler:
    """Load previously saved scaler."""
    if not SCALER_PATH.exists():
        raise FileNotFoundError(
            f"Scaler not found at {SCALER_PATH}. Run train_models.py first."
        )
    return joblib.load(SCALER_PATH)


def scale_features(X: np.ndarray, scaler: StandardScaler = None) -> np.ndarray:
    """Scale features. Loads scaler from disk if not provided."""
    if scaler is None:
        scaler = load_scaler()
    return scaler.transform(X)


# ── chronological train/test split ────────────────────────────────────────────

def chronological_split(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    val_ratio: float   = 0.15,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split by time to avoid future-data leakage.

    Returns (train_df, val_df, test_df) sorted chronologically.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    n = len(df)
    train_end = int(n * train_ratio)
    val_end   = int(n * (train_ratio + val_ratio))
    return df.iloc[:train_end], df.iloc[train_end:val_end], df.iloc[val_end:]


# ── single-event feature extraction (for streaming) ───────────────────────────

def extract_single_event_features(event: dict, history: list[dict]) -> dict:
    """
    Extract features for one streaming event given recent history.

    Parameters
    ----------
    event   : current chunk dict with keys matching labeled_trades.csv columns
    history : list of recent chunk dicts (same symbol), newest-last

    Returns
    -------
    dict mapping feature name → float
    """
    # compute_features uses a five-row rolling window including the current row.
    # Keep this inference path on the same window/statistics to avoid train/serve skew.
    prev = history[-(ROLLING_WINDOW - 1):]
    prices   = [float(h["price"]) for h in prev] + [float(event["price"])]
    volumes  = [float(h["volume"]) for h in prev] + [float(event["volume"])]

    def safe_pct_change(arr):
        if len(arr) < 2 or arr[-2] == 0:
            return 0.0
        return (arr[-1] - arr[-2]) / arr[-2]

    def safe_std(arr):
        return float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0

    volume     = event["volume"]
    n_tr       = max(event["n_trades"], 1)
    rush       = event.get("rush_orders", 0)
    buy_ratio  = event.get("buy_ratio", 0.5)

    vol_mean = float(np.mean(volumes))
    vol_std  = safe_std(volumes)
    if vol_std == 0:
        vol_std = 1e-8

    vol_prices = [float(h["price"]) for h in history[-ROLLING_WINDOW:]] + [float(event["price"])]
    price_changes = [0.0] + [
        (vol_prices[i] - vol_prices[i-1]) / vol_prices[i-1]
        for i in range(1, len(vol_prices)) if vol_prices[i-1] != 0
    ]

    price_change = float(np.clip(safe_pct_change(prices), -1.0, 10.0))
    volume_change = float(np.clip(safe_pct_change(volumes), -1.0, 50.0))

    return {
        "price_change_pct"  : price_change,
        "price_std"         : safe_std(prices),
        "volume"            : volume,
        "volume_change_pct" : volume_change,
        "volume_std"        : safe_std(volumes),
        "n_trades"          : event["n_trades"],
        "trade_freq"        : event["n_trades"] / CHUNK_SECONDS,
        "rush_order_ratio"  : rush / n_tr,
        "buy_ratio"         : buy_ratio,
        "avg_trade_size"    : volume / n_tr,
        "rolling_volatility": safe_std(price_changes[-ROLLING_WINDOW:]) if price_changes else 0.0,
        "anomaly_score"     : (volume - vol_mean) / vol_std,
    }


# ── CLI smoke test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from pathlib import Path as P
    csv = P(__file__).parent.parent.parent.parent / "data" / "pump_dump_dataset" / "labeled_trades.csv"
    if csv.exists():
        df = pd.read_csv(csv)
        out = compute_features(df.head(500))
        print("Feature columns:", FEATURE_COLS)
        print(out[FEATURE_COLS].describe().round(4))
        print(f"\nMissing values: {out[FEATURE_COLS].isnull().sum().sum()}")
    else:
        print("Run `python3 data/download.py` first to generate labeled_trades.csv")
