from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum
)

from feature_engineering import create_rfm_features


# Create Spark Session
spark = SparkSession.builder \
    .appName("Prepare_Linear_Regression_Data") \
    .master("local[*]") \
    .getOrCreate()


print("\n===== SPARK SESSION CREATED =====")

# Load the original dataset
df = spark.read.csv(
    "data/processed/online_retail.csv",
    header=True,
    inferSchema=True
)

print("\n===== DATASET LOADED =====")
print("Total Records:", df.count())

df.printSchema()

# Filter valid sales transactions
sales_df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (~col("InvoiceNo").startswith("C")) &
    (~col("InvoiceNo").startswith("A")) &
    (col("CustomerID").isNotNull())
)

# Calculate transaction revenue
sales_df = sales_df.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)

print("\n===== CLEANED SALES DATA =====")
print("Valid Sales Records:", sales_df.count())

sales_df.select(
    "InvoiceNo",
    "CustomerID",
    "Quantity",
    "UnitPrice",
    "Revenue"
).show(5)

# Define historical and future periods
cutoff_date = "2011-09-10"
future_end_date = "2011-12-09"

# Historical transactions
historical_df = sales_df.filter(
    col("InvoiceDate") <= cutoff_date
)

# Future transactions
future_df = sales_df.filter(
    (col("InvoiceDate") > cutoff_date) &
    (col("InvoiceDate") <= future_end_date)
)

print("\n===== HISTORICAL AND FUTURE DATA =====")
print("Historical Records:", historical_df.count())
print("Future Records:", future_df.count())

# Calculate historical RFM features
rfm_df = create_rfm_features(
    historical_df,
    snapshot_date="2011-09-10"
)

print("\n===== HISTORICAL RFM FEATURES =====")
print("Total Customers:", rfm_df.count())

rfm_df.show(5)

# Calculate future spending for each customer
future_spending_df = future_df.groupBy("CustomerID").agg(
    spark_sum("Revenue").alias("FutureSpend")
)

print("\n===== FUTURE CUSTOMER SPENDING =====")
print("Customers with Future Purchases:", future_spending_df.count())

future_spending_df.orderBy(
    col("FutureSpend").desc()
).show(10)

# Join historical RFM with future spending
linear_df = rfm_df.join(
    future_spending_df,
    on="CustomerID",
    how="left"
)

# Customers with no future purchases get zero spending
linear_df = linear_df.fillna(
    0,
    subset=["FutureSpend"]
)

print("\n===== FINAL LINEAR REGRESSION DATA =====")
print("Total Customers:", linear_df.count())

linear_df.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary",
    "FutureSpend"
).show(10)

# Save final dataset in Parquet format
linear_df.write \
    .mode("overwrite") \
    .parquet("data/processed/linear_customer_data")

print("\n===== LINEAR REGRESSION DATA SAVED =====")

# Stop Spark
spark.stop()