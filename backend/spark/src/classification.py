"""
XGBoost Pump-and-Dump Classifier
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Arnav (044) — Graph Analysis & Detection

Trains and evaluates an XGBoost classifier on the SystemsLab-Sapienza dataset.
Consumes features produced by Member 2 (Sruthi / preprocessing.py).
Uses chronological train/val/test split to prevent future-data leakage.
Saves the trained model and scaler to the models/ directory.
Provides explainable risk scores (feature-importance-based explanations).
"""

import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    auc,
    f1_score,
    precision_score,
    recall_score,
)
from xgboost import XGBClassifier

warnings.filterwarnings("ignore")

# ── paths ──────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent.parent.parent
DATA_DIR   = ROOT / "data" / "pump_dump_dataset"
MODEL_DIR  = ROOT / "models"
MODEL_PATH = MODEL_DIR / "xgboost_detector.joblib"
METRICS_PATH = MODEL_DIR / "eval_metrics.json"

RANDOM_SEED = 42

from backend.spark.src.preprocessing import (
    FEATURE_COLS,
    compute_features,
    chronological_split,
    fit_scaler,
    scale_features,
)


# ── training ───────────────────────────────────────────────────────────────────

def load_and_prepare(labeled_csv: Path) -> pd.DataFrame:
    """Load labeled_trades.csv and compute all features."""
    df = pd.read_csv(labeled_csv)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).reset_index(drop=True)
    df = compute_features(df)
    df = df.dropna(subset=FEATURE_COLS).reset_index(drop=True)
    return df


def train(df: pd.DataFrame):
    """
    Chronological train/val/test split, fit scaler on train only, train XGBoost.
    Returns (model, scaler, metrics_dict).
    """
    train_df, val_df, test_df = chronological_split(df)
    print(f"  Train: {len(train_df):,}  |  Val: {len(val_df):,}  |  Test: {len(test_df):,}")

    X_train = train_df[FEATURE_COLS].values
    y_train = train_df["is_pump"].values

    X_val   = val_df[FEATURE_COLS].values
    y_val   = val_df["is_pump"].values

    X_test  = test_df[FEATURE_COLS].values
    y_test  = test_df["is_pump"].values

    # Fit scaler on training set only
    scaler  = fit_scaler(X_train)
    X_train = scaler.transform(X_train)
    X_val   = scaler.transform(X_val)
    X_test  = scaler.transform(X_test)

    # Class imbalance weight
    n_pos = y_train.sum()
    n_neg = len(y_train) - n_pos
    scale_pos = n_neg / max(n_pos, 1)

    model = XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos,
        eval_metric="logloss",
        random_state=RANDOM_SEED,
        n_jobs=-1,
        verbosity=0,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    # ── evaluation on test set ─────────────────────────────────────────────────
    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    precision_vals, recall_vals, _ = precision_recall_curve(y_test, y_proba)
    pr_auc = auc(recall_vals, precision_vals)

    metrics = {
        "train_size"  : int(len(X_train)),
        "val_size"    : int(len(X_val)),
        "test_size"   : int(len(X_test)),
        "precision"   : round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
        "recall"      : round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
        "f1"          : round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        "pr_auc"      : round(float(pr_auc), 4),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        "feature_cols": FEATURE_COLS,
        "class_distribution": {
            "train_pump_ratio": round(float(y_train.mean()), 4),
            "test_pump_ratio" : round(float(y_test.mean()), 4),
        },
        "model": "XGBoost",
        "random_seed": RANDOM_SEED,
    }

    # Baseline: always-normal classifier
    baseline_f1 = f1_score(y_test, np.zeros_like(y_test), zero_division=0)
    metrics["baseline_f1_all_normal"] = round(float(baseline_f1), 4)

    # Feature importances
    importances = model.feature_importances_
    metrics["feature_importances"] = {
        FEATURE_COLS[i]: round(float(importances[i]), 4)
        for i in np.argsort(importances)[::-1]
    }

    return model, scaler, metrics


def save_model(model, metrics: dict):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2))
    print(f"  Model saved → {MODEL_PATH}")
    print(f"  Metrics saved → {METRICS_PATH}")


def load_model():
    """Load saved model. Raises FileNotFoundError if not trained yet."""
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. Run scripts/train_models.py first."
        )
    return joblib.load(MODEL_PATH)


# ── inference ──────────────────────────────────────────────────────────────────

def predict_risk(model, features: dict, scaler=None) -> dict:
    """
    Run inference on a single event's feature dict.

    Returns dict with:
      - predicted_class (0=NORMAL, 1=PUMP_AND_DUMP)
      - risk_score      (float 0–1)
      - label           (str)
      - explanation     (list of feature contributions)
      - threshold       (float, configurable)
    """
    if scaler is None:
        try:
            scaler = joblib.load(Path(__file__).parent.parent.parent.parent / "models" / "scaler.joblib")
        except FileNotFoundError:
            scaler = None

    x = np.array([[features.get(c, 0.0) for c in FEATURE_COLS]])
    if scaler is not None:
        x = scaler.transform(x)

    proba     = float(model.predict_proba(x)[0, 1])
    threshold = 0.50
    predicted = int(proba >= threshold)
    label     = "PUMP_AND_DUMP" if predicted == 1 else "NORMAL"

    # Simple explanation: top features contributing to this score
    raw = np.array([[features.get(c, 0.0) for c in FEATURE_COLS]])
    importances = model.feature_importances_
    contributions = sorted(
        [
            {
                "feature": FEATURE_COLS[i],
                "value"  : round(float(raw[0, i]), 6),
                "importance": round(float(importances[i]), 4),
            }
            for i in range(len(FEATURE_COLS))
        ],
        key=lambda d: d["importance"],
        reverse=True,
    )[:5]

    return {
        "predicted_class": predicted,
        "risk_score"      : round(proba, 4),
        "label"           : label,
        "threshold"       : threshold,
        "explanation"     : contributions,
    }


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    labeled_csv = DATA_DIR / "labeled_trades.csv"
    if not labeled_csv.exists():
        print("Run `python3 data/download.py` first.")
        raise SystemExit(1)

    print("=" * 55)
    print("  XGBoost Pump-and-Dump Classifier — Training")
    print("=" * 55)
    df = load_and_prepare(labeled_csv)
    print(f"  Dataset: {len(df):,} rows | Pump ratio: {df['is_pump'].mean():.2%}")

    model, scaler, metrics = train(df)
    save_model(model, metrics)

    print("\n  ── Test Set Metrics ──────────────────────────────────")
    print(f"  Precision : {metrics['precision']:.4f}")
    print(f"  Recall    : {metrics['recall']:.4f}")
    print(f"  F1        : {metrics['f1']:.4f}")
    print(f"  PR-AUC    : {metrics['pr_auc']:.4f}")
    print(f"  Baseline F1 (all-normal): {metrics['baseline_f1_all_normal']:.4f}")
    print("\n  Confusion Matrix:")
    cm = metrics["confusion_matrix"]
    print(f"    TN={cm[0][0]}  FP={cm[0][1]}")
    print(f"    FN={cm[1][0]}  TP={cm[1][1]}")
    print("\n  Top Features:")
    for f, v in list(metrics["feature_importances"].items())[:5]:
        print(f"    {f}: {v}")
