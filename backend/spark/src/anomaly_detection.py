"""
Spark Streaming - Classification with XGBoost + LightGBM
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Sruthi
Note: GAT (Graph Attention Network) runs in Neo4j module (Arnav)
"""

import numpy as np

# ML Model Configuration
FEATURES = [
    "price_change_pct", "volume_spike_ratio", "buy_sell_ratio",
    "trade_frequency", "order_cancel_rate", "bid_ask_spread",
    "wallet_concentration", "social_score", "telegram_msg_rate",
    "telegram_keyword_score", "hour_of_day", "day_of_week"
]

LABELS = {
    0: "NORMAL",
    1: "PUMP_AND_DUMP",
    2: "WASH_TRADE",
    3: "SPOOFING"
}

def load_training_data(csv_path):
    """Load training data from CSV."""
    import pandas as pd
    df = pd.read_csv(csv_path)
    X = df[FEATURES].values
    y = df["label"].values
    return X, y

def train_xgboost(X_train, y_train):
    """Train XGBoost classifier for manipulation detection."""
    from xgboost import XGBClassifier
    model = XGBClassifier(
        n_estimators=200,
        max_depth=8,
        learning_rate=0.1,
        use_label_encoder=False,
        eval_metric="mlogloss"
    )
    model.fit(X_train, y_train)
    return model

def train_lightgbm(X_train, y_train):
    """Train LightGBM for Telegram pump signal detection."""
    import lightgbm as lgb
    model = lgb.LGBMClassifier(
        n_estimators=150,
        max_depth=6,
        learning_rate=0.1
    )
    model.fit(X_train, y_train)
    return model

def predict(model, trade_features):
    """Predict manipulation type for a single trade."""
    features = np.array(trade_features).reshape(1, -1)
    prediction = model.predict(features)[0]
    return LABELS[prediction]

if __name__ == "__main__":
    print("=" * 50)
    print("  SPARK ML PIPELINE - CONFIGURATION")
    print("=" * 50)
    print(f"  Features: {len(FEATURES)}")
    print(f"  Models:   XGBoost + LightGBM (Spark) | GAT (Neo4j)")
    print(f"  Classes:  {list(LABELS.values())}")
    print("  Status:   Ready for training")
