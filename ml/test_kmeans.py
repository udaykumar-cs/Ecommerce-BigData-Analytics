import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import count, avg
from feature_engineering import (
    create_rfm_features,
    prepare_scaled_features
)

from kmeans_clustering import (
    train_kmeans,
    evaluate_kmeans,
    find_optimal_k,
    plot_kmeans_metrics
)

# Create Spark session
spark = SparkSession.builder \
    .appName("KMeans_Clustering_Test") \
    .master("local[*]") \
    .getOrCreate()


# Load processed dataset
df = spark.read.csv(
    "data/processed/online_retail.csv",
    header=True,
    inferSchema=True
)

print("\n===== DATASET LOADED =====")
print("Total rows:", df.count())

# Clean transaction records
sales_df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (~col("InvoiceNo").startswith("C")) &
    (~col("InvoiceNo").startswith("A"))
)

# Calculate Revenue
sales_df = sales_df.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)

print("\n===== CLEANED SALES DATA =====")
print("Total sales records:", sales_df.count())


# Generate RFM features
rfm_df = create_rfm_features(sales_df)

print("\n===== RFM FEATURES GENERATED =====")

rfm_df.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary"
).show(10)


# Scale RFM features
rfm_scaled_df, scaler_model = prepare_scaled_features(rfm_df)

print("\n===== SCALED RFM FEATURES =====")

rfm_scaled_df.select(
    "CustomerID",
    "ScaledFeatures"
).show(10, truncate=False)

# Find optimal number of clusters
print("\n===== FINDING OPTIMAL K =====")

results = find_optimal_k(rfm_scaled_df)

# Plot evaluation metrics
plot_kmeans_metrics(results)

# Train final K-Means model
print("\n===== TRAINING FINAL K-MEANS MODEL =====")

model, predictions = train_kmeans(
    rfm_scaled_df,
    k=3
)

print("\n===== K-MEANS CLUSTER CENTROIDS =====")

for index, center in enumerate(model.clusterCenters()):
    print(f"Cluster {index}: {center}")


print("\n===== CUSTOMER CLUSTER ASSIGNMENTS =====")

predictions.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary",
    "Cluster"
).show(20, truncate=False)

# Save trained models
os.makedirs("models", exist_ok=True)

scaler_model.write().overwrite().save("models/rfm_scaler")

model.write().overwrite().save("models/kmeans_model")

print("\n===== MODELS SAVED SUCCESSFULLY =====")

# Evaluate final model
silhouette = evaluate_kmeans(predictions)

print("\n===== FINAL MODEL EVALUATION =====")
print("Silhouette Score:", silhouette)


# Analyze customer segments
print("\n===== CUSTOMER SEGMENT ANALYSIS =====")

cluster_summary = predictions.groupBy("Cluster").agg(
    count("CustomerID").alias("Total Customers"),
    avg("Recency").alias("Average Recency"),
    avg("Frequency").alias("Average Frequency"),
    avg("Monetary").alias("Average Monetary")
)

cluster_summary.orderBy("Cluster").show(truncate=False)

# Inspect high-value customers
print("\n===== CLUSTER 2 CUSTOMER DETAILS =====")

predictions.filter(
    col("Cluster") == 2
).orderBy(
    col("Monetary").desc()
).select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary"
).show(20, truncate=False)


# Stop Spark session
spark.stop()