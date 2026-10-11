"""
Dataset Download & Preprocessing Script
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Nikunj (018) — Data Collection & Preprocessing

Downloads and preprocesses the SystemsLab-Sapienza Pump-and-Dump Benchmark Dataset.
Dataset: https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset
Paper:   "Pump and Dumps in the Bitcoin Era", ICCCN 2020

Role: This module covers Member 1's deliverable — dataset ingestion, cleaning,
timestamp normalisation, missing-value handling, and labeled_trades.csv generation.

NOTE on Telegram data: The Sapienza dataset records which Telegram groups organised
each pump (group code, date, hour). Full Telegram message text is not included.
Live Telegram integration remains future work and is NOT implemented in this prototype.

The dataset provides:
  - pump_telegram.csv  : labeled pump-and-dump events with coin, group, date, hour, exchange
  - group.csv          : Telegram group metadata

We generate synthetic-but-realistic trade-level OHLCV data aligned to each labeled
pump event for use in feature engineering and ML training, since the raw tick data
requires a Binance API key to download.
"""

import os
import sys
import requests
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent
DATASET_DIR = DATA_DIR / "pump_dump_dataset"

PUMP_TELEGRAM_URL = (
    "https://raw.githubusercontent.com/SystemsLab-Sapienza/"
    "pump-and-dump-dataset/master/pump_telegram.csv"
)
GROUP_URL = (
    "https://raw.githubusercontent.com/SystemsLab-Sapienza/"
    "pump-and-dump-dataset/master/group.csv"
)

# ── reproducibility ────────────────────────────────────────────────────────────
RANDOM_SEED = 42
RNG = np.random.default_rng(RANDOM_SEED)


def download_file(url: str, dest: Path) -> bool:
    """Download a file from a URL, return True on success."""
    try:
        print(f"  Downloading {dest.name} …", end=" ", flush=True)
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        print(f"OK ({len(r.content):,} bytes)")
        return True
    except Exception as e:
        print(f"FAILED — {e}")
        return False


def load_pump_events() -> pd.DataFrame:
    """Load pump_telegram.csv and return a clean DataFrame."""
    path = DATASET_DIR / "pump_telegram.csv"
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    # Normalise column names seen in different versions of the file
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("symbol", "sym", "coin"):
            col_map[c] = "symbol"
        elif lc in ("group", "group_code"):
            col_map[c] = "group"
        elif lc in ("date",):
            col_map[c] = "date"
        elif lc in ("hour", "time"):
            col_map[c] = "hour"
        elif lc in ("exchange",):
            col_map[c] = "exchange"
    df = df.rename(columns=col_map)

    required = {"symbol", "date", "hour"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"pump_telegram.csv missing columns: {missing}")

    df["symbol"] = df["symbol"].str.upper().str.strip()
    df["pump_time"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["hour"].astype(str),
        utc=True, errors="coerce"
    )
    df = df.dropna(subset=["pump_time"]).reset_index(drop=True)
    return df


def generate_trade_window(
    symbol: str,
    pump_time: pd.Timestamp,
    is_pump: bool,
    window_seconds: int = 600,
    chunk_size: int = 5,
    base_price: float = None,
) -> pd.DataFrame:
    """
    Generate a realistic synthetic trade window around a pump event.

    Designed to reflect the actual signal-to-noise ratio observed in the
    Sapienza dataset analysis (ICCCN 2020): pump chunks have elevated trade
    counts, rush orders, and buy-side pressure, but there is meaningful overlap
    with high-activity normal periods so the classifier must genuinely learn.

    Parameters
    ----------
    symbol        : trading pair symbol, e.g. "BTC"
    pump_time     : UTC timestamp of the pump signal
    is_pump       : whether this window is labelled pump=True
    window_seconds: total window length in seconds
    chunk_size    : aggregation interval in seconds (5 sec matches Sapienza)
    base_price    : approximate price level; randomised if None
    """
    if base_price is None:
        base_price = RNG.uniform(0.00001, 0.01)   # SYM/BTC pair price range

    n_chunks = window_seconds // chunk_size
    timestamps = [
        pump_time - pd.Timedelta(seconds=(window_seconds - i * chunk_size))
        for i in range(n_chunks)
    ]
    # Pump begins in the last 20 % of the window (pre-pump buildup exists)
    pump_start_idx = int(n_chunks * 0.80)

    # Normal baseline: Poisson(15..30 trades/chunk) — small-cap altcoin on Binance
    baseline_lam = RNG.uniform(15, 30)

    rows = []
    price = base_price
    for i, ts in enumerate(timestamps):
        in_pump = is_pump and i >= pump_start_idx

        if in_pump:
            # Pump surge: 2-5× normal trade count with realistic noise
            surge = RNG.uniform(2.0, 5.0)
            n_trades = int(RNG.poisson(lam=baseline_lam * surge))
            # Volume spike: 3-8× normal (BTC terms)
            vol_multiplier = RNG.uniform(3.0, 8.0)
            base_vol = RNG.exponential(scale=0.3)
            volume = base_vol * vol_multiplier
            # Price spike: up to +20 % at peak then partial reversal
            progress = (i - pump_start_idx) / max(n_chunks - pump_start_idx, 1)
            spike = 0.20 * np.sin(np.pi * progress)
            price = price * (1.0 + spike + RNG.normal(0, 0.008))
            # Rush orders: elevated but noisy — real pumps sometimes fail to spike clearly
            rush_ratio = RNG.beta(3, 2)    # mean ~0.60, range 0-1
            buy_ratio  = RNG.beta(5, 2)    # mean ~0.71, range 0-1
        else:
            # Normal period: mild noise; fat-tail chunks add realistic confusion
            fat_tail = RNG.random() < 0.08
            lam = baseline_lam * (RNG.uniform(1.5, 3.0) if fat_tail else 1.0)
            n_trades = int(RNG.poisson(lam=lam))
            base_vol = RNG.exponential(scale=0.3)
            volume = base_vol * (RNG.uniform(1.2, 3.5) if fat_tail else 1.0)
            price = price * (1.0 + RNG.normal(0, 0.003))
            rush_ratio = RNG.beta(1, 5)    # mean ~0.17 — heavy overlap in tail
            buy_ratio  = RNG.beta(2, 2)    # mean 0.50, wide spread

        n_trades  = max(n_trades, 1)
        price     = max(price, 1e-8)
        rush_orders = int(n_trades * rush_ratio)

        label = 1 if in_pump else 0

        rows.append({
            "timestamp": ts,
            "symbol": symbol,
            "price": round(price, 10),
            "volume": round(volume, 6),
            "n_trades": n_trades,
            "rush_orders": rush_orders,
            "buy_ratio": round(buy_ratio, 4),
            "is_pump": label,
        })
    return pd.DataFrame(rows)


def build_labeled_dataset(
    pump_events: pd.DataFrame,
    max_events: int = 200,
    normal_multiplier: int = 3,
) -> pd.DataFrame:
    """
    Build a fully-labeled trade-chunk dataset from pump events.

    For each pump event we generate:
      - one pump window (labeled 1)
      - `normal_multiplier` normal windows from the same coin (labeled 0)
    """
    pump_events = pump_events.head(max_events)
    print(f"  Building labeled dataset from {len(pump_events)} pump events …")

    all_rows = []
    for _, row in pump_events.iterrows():
        symbol = row["symbol"]
        pump_time = row["pump_time"]
        base_price = RNG.uniform(0.000005, 0.02)

        # Pump window
        df_pump = generate_trade_window(symbol, pump_time, is_pump=True,
                                        base_price=base_price)
        df_pump["event_id"] = f"{symbol}_{pump_time.strftime('%Y%m%d_%H%M%S')}_pump"
        df_pump["group"] = row.get("group", "unknown")
        df_pump["exchange"] = row.get("exchange", "binance")
        all_rows.append(df_pump)

        # Normal windows (different times)
        for j in range(normal_multiplier):
            offset_days = RNG.integers(1, 30) * (-1 if j % 2 == 0 else 1)
            normal_time = pump_time + pd.Timedelta(days=int(offset_days))
            df_normal = generate_trade_window(
                symbol, normal_time, is_pump=False, base_price=base_price
            )
            df_normal["event_id"] = (
                f"{symbol}_{normal_time.strftime('%Y%m%d_%H%M%S')}_normal_{j}"
            )
            df_normal["group"] = row.get("group", "unknown")
            df_normal["exchange"] = row.get("exchange", "binance")
            all_rows.append(df_normal)

    df = pd.concat(all_rows, ignore_index=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    print(f"  Total rows: {len(df):,}  |  Pump chunks: {df['is_pump'].sum():,}  |  "
          f"Normal chunks: {(df['is_pump'] == 0).sum():,}")
    return df


def main():
    print("=" * 60)
    print("  Pump-and-Dump Dataset Setup")
    print("  Source: SystemsLab-Sapienza (ICCCN 2020)")
    print("=" * 60)

    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Download raw label files
    ok1 = download_file(PUMP_TELEGRAM_URL, DATASET_DIR / "pump_telegram.csv")
    ok2 = download_file(GROUP_URL, DATASET_DIR / "group.csv")

    if not ok1:
        print("\n  ⚠  Could not download pump_telegram.csv.")
        print("  Generating synthetic fallback dataset …")
        _generate_synthetic_fallback()
        return

    # 2. Parse pump events
    pump_events = load_pump_events()
    print(f"\n  Loaded {len(pump_events)} labeled pump events")
    print(f"  Coins:  {pump_events['symbol'].nunique()} unique")
    if "exchange" in pump_events.columns:
        print(f"  Exchanges: {pump_events['exchange'].unique().tolist()[:5]}")

    # 3. Save clean events list
    pump_events.to_csv(DATASET_DIR / "pump_events_clean.csv", index=False)
    print("  Saved pump_events_clean.csv")

    # 4. Build labeled trade-chunk dataset
    labeled = build_labeled_dataset(pump_events, max_events=200, normal_multiplier=3)
    out_path = DATASET_DIR / "labeled_trades.csv"
    labeled.to_csv(out_path, index=False)
    print(f"  Saved labeled_trades.csv  ({len(labeled):,} rows)")

    if ok2:
        try:
            groups = pd.read_csv(DATASET_DIR / "group.csv")
            print(f"\n  Groups loaded: {len(groups)} Telegram P&D groups")
        except Exception:
            pass

    print("\n✅  Dataset ready.")
    print(f"   Location: {DATASET_DIR}")
    print(f"   Files:    pump_telegram.csv, group.csv, pump_events_clean.csv, labeled_trades.csv")


def _generate_synthetic_fallback():
    """Generate a fully synthetic dataset when network is unavailable."""
    print("  Generating synthetic pump-and-dump dataset (offline fallback) …")
    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    coins = ["BTC", "ETH", "XRP", "LTC", "ADA", "DOGE", "DOT", "LINK", "UNI", "AVAX"]
    groups = ["BigPumpSignal", "CryptoMoonSignals", "XVIPump", "PumpKingdom"]
    exchanges = ["binance"]
    dates = pd.date_range("2019-01-01", "2020-12-31", freq="3D")

    pump_rows = []
    for i, d in enumerate(dates[:200]):
        pump_rows.append({
            "symbol": RNG.choice(coins),
            "group": RNG.choice(groups),
            "date": d.strftime("%Y-%m-%d"),
            "hour": f"{RNG.integers(0,24):02d}:{RNG.integers(0,60):02d}",
            "exchange": RNG.choice(exchanges),
        })
    pump_df = pd.DataFrame(pump_rows)
    pump_df.to_csv(DATASET_DIR / "pump_telegram.csv", index=False)

    group_df = pd.DataFrame({
        "group_name": groups,
        "group_code": ["BPS", "CMS", "XVIP", "PK"],
        "last_time_checked": ["2020-12-31"] * 4,
        "telegram_link": ["https://t.me/example"] * 4,
    })
    group_df.to_csv(DATASET_DIR / "group.csv", index=False)
    print("  Created synthetic pump_telegram.csv and group.csv")

    pump_events = pump_df.copy()
    pump_events["pump_time"] = pd.to_datetime(
        pump_events["date"] + " " + pump_events["hour"], utc=True, errors="coerce"
    )
    pump_events = pump_events.dropna(subset=["pump_time"])
    pump_events.to_csv(DATASET_DIR / "pump_events_clean.csv", index=False)

    labeled = build_labeled_dataset(pump_events, max_events=200, normal_multiplier=3)
    labeled.to_csv(DATASET_DIR / "labeled_trades.csv", index=False)
    print(f"  Generated labeled_trades.csv  ({len(labeled):,} rows)")
    print("\n✅  Synthetic fallback dataset ready.")


if __name__ == "__main__":
    main()
