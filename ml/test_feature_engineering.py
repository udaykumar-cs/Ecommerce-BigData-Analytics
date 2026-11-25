from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from feature_engineering import (
    create_rfm_features,
    prepare_scaled_features
)


# Create Spark session
spark = SparkSession.builder \
    .appName("Feature_Engineering_Test") \
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

sales_df.select(
    "CustomerID",
    "Quantity",
    "UnitPrice",
    "Revenue"
).show(5)

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
    "Features",
    "ScaledFeatures"
).show(10, truncate=False)


# Stop Spark session
spark.stop()