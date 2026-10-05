"""
Neo4j Graph Builder + GAT (Graph Attention Network)
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Arnav
"""

import numpy as np

# Neo4j Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Graph Algorithms + GAT
ALGORITHMS = {
    "wash_trade_detection": "GAT — learns circular wallet patterns",
    "pump_group_detection": "GAT — finds coordinated buying groups",
    "central_wallet_finder": "PageRank + GAT Attention Scores"
}

# GAT Model Configuration
GAT_CONFIG = {
    "num_heads": 4,           # Multi-head attention
    "hidden_channels": 64,    # Hidden layer size
    "num_layers": 2,          # GAT layers
    "dropout": 0.3,
    "num_classes": 2,         # Suspicious / Normal
    "learning_rate": 0.005,
    "epochs": 100
}

def connect_neo4j():
    """Connect to Neo4j database."""
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return driver

def create_wallet_node(tx, wallet_id):
    """Create a wallet node in the graph."""
    tx.run("MERGE (w:Wallet {id: $wallet_id})", wallet_id=wallet_id)

def create_transaction_edge(tx, sender, receiver, amount, coin, timestamp):
    """Create a transaction edge between two wallets."""
    tx.run("""
        MATCH (s:Wallet {id: $sender}), (r:Wallet {id: $receiver})
        CREATE (s)-[:SENT {amount: $amount, coin: $coin, timestamp: $timestamp}]->(r)
    """, sender=sender, receiver=receiver, amount=amount, coin=coin, timestamp=timestamp)

def build_gat_model():
    """Build GAT model using PyTorch Geometric."""
    import torch
    import torch.nn.functional as F
    from torch_geometric.nn import GATConv

    class CryptoGAT(torch.nn.Module):
        def __init__(self, in_channels, hidden_channels, out_channels, heads=4, dropout=0.3):
            super().__init__()
            self.conv1 = GATConv(in_channels, hidden_channels, heads=heads, dropout=dropout)
            self.conv2 = GATConv(hidden_channels * heads, out_channels, heads=1, concat=False, dropout=dropout)
            self.dropout = dropout

        def forward(self, x, edge_index):
            x = F.dropout(x, p=self.dropout, training=self.training)
            x = self.conv1(x, edge_index)
            x = F.elu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
            x = self.conv2(x, edge_index)
            return x

    model = CryptoGAT(
        in_channels=12,
        hidden_channels=GAT_CONFIG["hidden_channels"],
        out_channels=GAT_CONFIG["num_classes"],
        heads=GAT_CONFIG["num_heads"],
        dropout=GAT_CONFIG["dropout"]
    )
    return model

def train_gat(model, data, epochs=100):
    """Train GAT model on wallet transaction graph."""
    import torch
    import torch.nn.functional as F

    optimizer = torch.optim.Adam(model.parameters(), lr=GAT_CONFIG["learning_rate"])
    model.train()

    for epoch in range(epochs):
        optimizer.zero_grad()
        out = model(data.x, data.edge_index)
        loss = F.cross_entropy(out[data.train_mask], data.y[data.train_mask])
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{epochs} | Loss: {loss.item():.4f}")

    return model

def predict_suspicious_wallets(model, data):
    """Predict which wallets are suspicious using trained GAT."""
    import torch

    model.eval()
    with torch.no_grad():
        out = model(data.x, data.edge_index)
        predictions = out.argmax(dim=1)
    
    suspicious = (predictions == 1).sum().item()
    total = len(predictions)
    print(f"\n  🚨 Suspicious wallets: {suspicious}/{total}")
    return predictions

def detect_wash_trade_cycles(tx):
    """Find circular transaction patterns: A → B → C → A."""
    result = tx.run("""
        MATCH path = (a:Wallet)-[:SENT*3..5]->(a)
        RETURN nodes(path) AS cycle_wallets, length(path) AS cycle_length
        LIMIT 100
    """)
    return [record.data() for record in result]

if __name__ == "__main__":
    print("=" * 55)
    print("  NEO4J + GAT GRAPH ANALYTICS - CONFIGURATION")
    print("=" * 55)
    print(f"  Neo4j URI:    {NEO4J_URI}")
    print(f"  GAT Heads:    {GAT_CONFIG['num_heads']}")
    print(f"  GAT Layers:   {GAT_CONFIG['num_layers']}")
    print(f"  Hidden Size:  {GAT_CONFIG['hidden_channels']}")
    print(f"  Classes:      Suspicious / Normal")
    print(f"\n  Algorithms:")
    for name, algo in ALGORITHMS.items():
        print(f"    → {name}: {algo}")
    print("\n  Status: Ready for graph construction + GAT training")
