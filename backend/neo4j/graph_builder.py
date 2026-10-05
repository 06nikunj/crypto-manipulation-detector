"""
Neo4j Graph Builder - Transaction Network Analysis
Project: Real-Time Cryptocurrency Market Manipulation Detection
Author: Arnav
"""

# Neo4j Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Graph Algorithms
ALGORITHMS = {
    "wash_trade_detection": "Strongly Connected Components (SCC)",
    "pump_group_detection": "Louvain Community Detection",
    "central_wallet_finder": "PageRank"
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

def detect_wash_trade_cycles(tx):
    """Find circular transaction patterns: A → B → C → A."""
    result = tx.run("""
        MATCH path = (a:Wallet)-[:SENT*3..5]->(a)
        RETURN nodes(path) AS cycle_wallets, length(path) AS cycle_length
        LIMIT 100
    """)
    return [record.data() for record in result]

def detect_pump_communities(tx):
    """Find coordinated buying groups using Louvain algorithm."""
    result = tx.run("""
        CALL gds.louvain.stream('wallet-graph')
        YIELD nodeId, communityId
        RETURN gds.util.asNode(nodeId).id AS wallet, communityId
        ORDER BY communityId
    """)
    return [record.data() for record in result]

if __name__ == "__main__":
    print("=" * 50)
    print("  NEO4J GRAPH BUILDER - CONFIGURATION")
    print("=" * 50)
    print(f"  URI:        {NEO4J_URI}")
    print(f"  Algorithms: {len(ALGORITHMS)}")
    for name, algo in ALGORITHMS.items():
        print(f"    → {name}: {algo}")
    print("  Status:     Ready for graph construction")
