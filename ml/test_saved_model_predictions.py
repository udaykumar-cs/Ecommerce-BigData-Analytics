from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml.clustering import KMeansModel

from feature_engineering import create_rfm_features


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Saved_Model_Predictions") \
    .master("local[*]") \
    .getOrCreate()


# 2. Load Dataset
df = spark.read.csv(
    "data/processed/online_retail.csv",
    header=True,
    inferSchema=True
)


# 3. Prepare Sales Data
sales_df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (~col("InvoiceNo").startswith("C")) &
    (~col("InvoiceNo").startswith("A"))
)

sales_df = sales_df.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)


# 4. Generate RFM Features
rfm_df = create_rfm_features(sales_df)


# 5. Load Previously Saved Models
scaler_model = StandardScalerModel.load("models/rfm_scaler")

kmeans_model = KMeansModel.load("models/kmeans_model")


# 6. Convert RFM Columns into Feature Vector
assembler = VectorAssembler(
    inputCols=["Recency", "Frequency", "Monetary"],
    outputCol="Features"
)

rfm_vector_df = assembler.transform(rfm_df)


# 7. Apply Previously Trained Scaler
scaled_df = scaler_model.transform(rfm_vector_df)


# 8. Predict Customer Clusters
predictions = kmeans_model.transform(scaled_df)


# 9. Display Predictions
print("\n===== CUSTOMER CLUSTER PREDICTIONS =====")

predictions.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary",
    "Cluster"
).show(20, truncate=False)


# 10. Display Cluster Distribution
print("\n===== PREDICTED CLUSTER DISTRIBUTION =====")

predictions.groupBy("Cluster").agg(
    count("*").alias("Total Customers")
).orderBy("Cluster").show()


# 11. Stop Spark
spark.stop()