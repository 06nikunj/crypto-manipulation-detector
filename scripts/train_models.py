"""
Model Training Script
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Arnav (044) — Graph Analysis & Detection

Trains the XGBoost pump-and-dump detector on the feature-engineered dataset.
Consumes features from Member 2 (Sruthi) and saves model + metrics to models/.

Usage:
  python3 scripts/train_models.py [--force]
"""

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

DATA_CSV   = ROOT / "data" / "pump_dump_dataset" / "labeled_trades.csv"
MODEL_PATH = ROOT / "models" / "xgboost_detector.joblib"


def main(force: bool = False):
    # ── guard: dataset must exist ──────────────────────────────────────────────
    if not DATA_CSV.exists():
        print(f"[ERROR] Dataset not found: {DATA_CSV}")
        print("       Run `python3 data/download.py` first.")
        sys.exit(1)

    # ── guard: skip if model already current ──────────────────────────────────
    if MODEL_PATH.exists() and not force:
        print(f"✓ Model already exists at {MODEL_PATH}")
        print("  Use --force to retrain.")
        _print_existing_metrics()
        return

    print("=" * 60)
    print("  XGBoost Pump-and-Dump Detector — Training")
    print(f"  Dataset: {DATA_CSV}")
    print("=" * 60)

    from backend.spark.src.classification import load_and_prepare, train, save_model
    import pandas as pd

    t0 = time.time()
    df = load_and_prepare(DATA_CSV)
    print(f"\n  Loaded {len(df):,} rows | "
          f"Pump ratio: {df['is_pump'].mean():.2%} | "
          f"Symbols: {df['symbol'].nunique()}")

    model, scaler, metrics = train(df)
    save_model(model, metrics)

    elapsed = time.time() - t0
    print(f"\n  Training time: {elapsed:.1f}s")
    print("\n  ── Evaluation on Chronological Test Set ──────────────────")
    print(f"  Precision : {metrics['precision']:.4f}")
    print(f"  Recall    : {metrics['recall']:.4f}")
    print(f"  F1        : {metrics['f1']:.4f}")
    print(f"  PR-AUC    : {metrics['pr_auc']:.4f}")
    print(f"  Baseline F1 (all-normal): {metrics['baseline_f1_all_normal']:.4f}")
    print("\n  Confusion Matrix:")
    cm = metrics["confusion_matrix"]
    print(f"    TN={cm[0][0]:>6}  FP={cm[0][1]:>6}")
    print(f"    FN={cm[1][0]:>6}  TP={cm[1][1]:>6}")
    print("\n  Top Feature Importances:")
    for feat, imp in list(metrics["feature_importances"].items())[:5]:
        bar = "█" * int(imp * 40)
        print(f"    {feat:<25} {bar} {imp:.4f}")

    print(f"\n✅  Model saved → {MODEL_PATH}")
    print(f"   Metrics saved → {ROOT / 'models' / 'eval_metrics.json'}")


def _print_existing_metrics():
    metrics_path = ROOT / "models" / "eval_metrics.json"
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text())
        print("\n  ── Saved Metrics ─────────────────────────────────────────")
        print(f"  Precision : {metrics.get('precision', 'N/A')}")
        print(f"  Recall    : {metrics.get('recall', 'N/A')}")
        print(f"  F1        : {metrics.get('f1', 'N/A')}")
        print(f"  PR-AUC    : {metrics.get('pr_auc', 'N/A')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Force retrain even if model exists")
    args = parser.parse_args()
    main(force=args.force)
