import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# 1. Load processed retail dataset
df = pd.read_csv("data/processed/online_retail.csv")

# 2. Clean required columns
df = df.dropna(subset=["CustomerID", "StockCode"])
df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]

# 3. Create customer-product purchase graph
G = nx.Graph()

for _, row in df.iterrows():
    customer = f"C_{int(row['CustomerID'])}"
    product = f"P_{row['StockCode']}"

    if G.has_edge(customer, product):
        G[customer][product]["weight"] += int(row["Quantity"])
    else:
        G.add_edge(
            customer,
            product,
            weight=int(row["Quantity"])
        )

# 4. Calculate graph statistics
print("\n===== CUSTOMER PRODUCT GRAPH =====")
print("Total Nodes:", G.number_of_nodes())
print("Total Edges:", G.number_of_edges())

customer_nodes = [
    node for node in G.nodes if node.startswith("C_")
]

product_nodes = [
    node for node in G.nodes if node.startswith("P_")
]

print("Customer Nodes:", len(customer_nodes))
print("Product Nodes:", len(product_nodes))

# 5. Identify most connected customers
degree = dict(G.degree())

top_customers = sorted(
    [(node, degree[node]) for node in customer_nodes],
    key=lambda x: x[1],
    reverse=True
)[:10]

print("\n===== TOP 10 CUSTOMERS BY PRODUCT CONNECTIONS =====")

for customer, connections in top_customers:
    print(customer, "->", connections, "products")

# 6. Visualize a manageable subgraph
selected_customers = [node for node, _ in top_customers[:5]]

selected_nodes = set(selected_customers)

for customer in selected_customers:
    selected_nodes.update(G.neighbors(customer))

subgraph = G.subgraph(selected_nodes)

plt.figure(figsize=(14, 10))

pos = nx.spring_layout(subgraph, seed=42, k=0.5)

colors = [
    "skyblue" if node.startswith("C_") else "orange"
    for node in subgraph.nodes
]

nx.draw(
    subgraph,
    pos,
    with_labels=True,
    node_color=colors,
    node_size=700,
    font_size=7,
    edge_color="gray"
)

plt.title("Customer-Product Purchase Network")
plt.savefig("results/customer_product_graph.png", dpi=300)
plt.show()

print("\nGraph saved to results/customer_product_graph.png")