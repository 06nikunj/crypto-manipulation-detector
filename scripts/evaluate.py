"""
Model Evaluation Script
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Sruthi Divakarla (Spark + ML Team)

Re-runs evaluation on the test split and prints full diagnostics.
Does NOT retrain the model.

Usage:
  python3 scripts/evaluate.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

DATA_CSV     = ROOT / "data" / "pump_dump_dataset" / "labeled_trades.csv"
MODEL_PATH   = ROOT / "models" / "xgboost_detector.joblib"
METRICS_PATH = ROOT / "models" / "eval_metrics.json"


def main():
    print("=" * 60)
    print("  Pump-and-Dump Detector — Evaluation Report")
    print("=" * 60)

    if not MODEL_PATH.exists():
        print("[ERROR] No trained model found. Run scripts/train_models.py first.")
        sys.exit(1)

    if not DATA_CSV.exists():
        print("[ERROR] Dataset not found. Run python3 data/download.py first.")
        sys.exit(1)

    import joblib
    import numpy as np
    from sklearn.metrics import (
        classification_report, confusion_matrix,
        precision_recall_curve, auc, roc_auc_score,
    )
    from backend.spark.src.classification import load_and_prepare
    from backend.spark.src.preprocessing import (
        FEATURE_COLS, chronological_split
    )

    print("\n  Loading dataset …")
    df = load_and_prepare(DATA_CSV)
    _, _, test_df = chronological_split(df)

    model  = joblib.load(MODEL_PATH)
    scaler = joblib.load(ROOT / "models" / "scaler.joblib")

    X_test = scaler.transform(test_df[FEATURE_COLS].values)
    y_test = test_df["is_pump"].values

    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    # ── classification report ──────────────────────────────────────────────────
    print("\n  ── Classification Report ────────────────────────────────")
    report = classification_report(
        y_test, y_pred,
        target_names=["NORMAL", "PUMP_AND_DUMP"],
        zero_division=0,
    )
    for line in report.split("\n"):
        print("  " + line)

    # ── confusion matrix ───────────────────────────────────────────────────────
    cm = confusion_matrix(y_test, y_pred)
    print("\n  ── Confusion Matrix ─────────────────────────────────────")
    print(f"  (rows = actual, cols = predicted)")
    print(f"              Normal   Pump")
    print(f"  Normal      {cm[0][0]:>6}   {cm[0][1]:>4}")
    print(f"  Pump        {cm[1][0]:>6}   {cm[1][1]:>4}")

    # ── PR-AUC ────────────────────────────────────────────────────────────────
    precision_vals, recall_vals, _ = precision_recall_curve(y_test, y_proba)
    pr_auc = auc(recall_vals, precision_vals)
    print(f"\n  PR-AUC : {pr_auc:.4f}")

    try:
        roc = roc_auc_score(y_test, y_proba)
        print(f"  ROC-AUC: {roc:.4f}")
    except Exception:
        pass

    # ── baseline ──────────────────────────────────────────────────────────────
    from sklearn.metrics import f1_score
    baseline_f1 = f1_score(y_test, np.zeros_like(y_test), zero_division=0)
    model_f1    = f1_score(y_test, y_pred, zero_division=0)
    print(f"\n  ── Baseline Comparison ──────────────────────────────────")
    print(f"  All-normal F1 (baseline): {baseline_f1:.4f}")
    print(f"  XGBoost F1  (our model):  {model_f1:.4f}")
    improvement = model_f1 - baseline_f1
    print(f"  Improvement:              {improvement:+.4f}")

    # ── feature importances ────────────────────────────────────────────────────
    print("\n  ── Feature Importances ──────────────────────────────────")
    importances = model.feature_importances_
    ranked = sorted(
        zip(FEATURE_COLS, importances), key=lambda t: t[1], reverse=True
    )
    for feat, imp in ranked:
        bar = "█" * int(imp * 50)
        print(f"  {feat:<25} {bar} {imp:.4f}")

    # ── class distribution ────────────────────────────────────────────────────
    print(f"\n  ── Test Set Class Distribution ──────────────────────────")
    n_pos = y_test.sum()
    n_neg = len(y_test) - n_pos
    print(f"  Normal  : {n_neg:>6} ({n_neg/len(y_test):.1%})")
    print(f"  Pump    : {n_pos:>6} ({n_pos/len(y_test):.1%})")

    print("\n  ── Environment ──────────────────────────────────────────")
    print(f"  Test set rows : {len(test_df):,}")
    print(f"  Model         : XGBoost (chronological train/val/test split)")
    print(f"  Dataset       : SystemsLab-Sapienza Pump-and-Dump (ICCCN 2020)")

    print("\n✅  Evaluation complete.")


if __name__ == "__main__":
    main()
