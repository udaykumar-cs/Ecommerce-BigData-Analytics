from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.metrics import silhouette_score
import numpy as np
import matplotlib.pyplot as plt

from scipy.cluster.hierarchy import (
    linkage,
    dendrogram,
    fcluster
)

from sklearn.preprocessing import StandardScaler

spark = SparkSession.builder \
    .appName("Hierarchical_Clustering") \
    .master("local[*]") \
    .getOrCreate()

print("\n===== SPARK SESSION CREATED =====")

# Load transaction dataset

df = spark.read.csv(
    "data/processed/online_retail.csv",
    header=True,
    inferSchema=True
)

# Clean sales records

sales_df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (~col("InvoiceNo").startswith("C")) &
    (~col("InvoiceNo").startswith("A"))
)

# Calculate revenue

from pyspark.sql.functions import col

sales_df = sales_df.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)

# Generate RFM features

from feature_engineering import create_rfm_features

rfm_df = create_rfm_features(sales_df)

print("\n===== RFM DATASET CREATED =====")

print("Total Customers:", rfm_df.count())

rfm_df.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary"
).show(5)

# Convert customer-level RFM data to Pandas

rfm_pd = rfm_df.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary"
).toPandas()

# Extract numerical features

features = rfm_pd[
    ["Recency", "Frequency", "Monetary"]
].to_numpy()

# Standardize features

scaler = StandardScaler()

scaled_features = scaler.fit_transform(features)

print("\n===== STANDARDIZED RFM FEATURES =====")

print("Feature Shape:", scaled_features.shape)

print("First 5 standardized records:")
print(scaled_features[:5])

# Perform Hierarchical Clustering

print("\n===== BUILDING HIERARCHICAL CLUSTER TREE =====")

linked = linkage(
    scaled_features,
    method="ward"
)

print("Hierarchical clustering completed.")

print("Linkage Matrix Shape:", linked.shape)

print("\nFirst 5 linkage operations:")
print(linked[:5])


# Generate Dendrogram

plt.figure(figsize=(14, 7))

dendrogram(
    linked,
    truncate_mode="lastp",
    p=30,
    show_leaf_counts=True
)

plt.title("Hierarchical Clustering Dendrogram")
plt.xlabel("Customer Clusters")
plt.ylabel("Ward Distance")

plt.tight_layout()

plt.savefig(
    "results/hierarchical_dendrogram.png",
    dpi=300
)

plt.show()


# Create 3 Hierarchical Clusters

cluster_labels = fcluster(
    linked,
    t=3,
    criterion="maxclust"
)

# Evaluate Hierarchical Clustering

score = silhouette_score(
    scaled_features,
    cluster_labels,
    metric="sqeuclidean"
)

print("\n===== HIERARCHICAL CLUSTER EVALUATION =====")
print("Silhouette Score:", round(score, 4))

rfm_pd["Cluster"] = cluster_labels

print("\n===== HIERARCHICAL CLUSTER ASSIGNMENTS =====")

print(rfm_pd[[
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary",
    "Cluster"
]].head(10))

print("\n===== CLUSTER DISTRIBUTION =====")

print(rfm_pd["Cluster"].value_counts().sort_index())


# Analyze Hierarchical Cluster Profiles

cluster_summary = rfm_pd.groupby("Cluster").agg(
    Customers=("CustomerID", "count"),
    Avg_Recency=("Recency", "mean"),
    Avg_Frequency=("Frequency", "mean"),
    Avg_Monetary=("Monetary", "mean"),
    Median_Monetary=("Monetary", "median")
).round(2)

print("\n===== HIERARCHICAL CLUSTER ANALYSIS =====")
print(cluster_summary)


# Save Hierarchical Clustering Results

rfm_pd.to_csv(
    "results/hierarchical_customer_segments.csv",
    index=False
)

np.save(
    "results/hierarchical_linkage.npy",
    linked
)

print("\n===== HIERARCHICAL RESULTS SAVED =====")