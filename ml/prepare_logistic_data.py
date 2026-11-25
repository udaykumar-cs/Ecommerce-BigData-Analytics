from feature_engineering import create_rfm_features
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lit, when, count
)
# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("Logistic_Regression_Data_Preparation") \
    .master("local[*]") \
    .getOrCreate()
# 2. Load Dataset
df = spark.read.csv(
    "data/processed/online_retail.csv",
    header=True,
    inferSchema=True
)
# 3. Clean Transaction Records
sales_df = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0) &
    (~col("InvoiceNo").startswith("C")) &
    (~col("InvoiceNo").startswith("A")) &
    (col("CustomerID").isNotNull())
)
# 4. Convert InvoiceDate into Date
sales_df = sales_df.withColumn(
    "TransactionDate",
    to_date(col("InvoiceDate"))
)
# Calculate Revenue
sales_df = sales_df.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)
# 5. Define Historical and Future Periods
cutoff_date = "2011-09-10"
future_end_date = "2011-12-09"

# Historical transactions
historical_df = sales_df.filter(
    col("TransactionDate") <= lit(cutoff_date)
)
# Future transactions
future_df = sales_df.filter(
    (col("TransactionDate") > lit(cutoff_date)) &
    (col("TransactionDate") <= lit(future_end_date))
)
# 6. Get Historical Customers
historical_customers = historical_df.select(
    "CustomerID"
).distinct()

# 7. Find Customers Who Purchased Again
future_customers = future_df.select(
    "CustomerID"
).distinct().withColumn(
    "Label", lit(1)
)
# 8. Create Classification Labels
labeled_customers = historical_customers.join(
    future_customers,
    on="CustomerID",
    how="left"
).fillna(0, subset=["Label"])
\
# 9. Generate Historical RFM Features
rfm_df = create_rfm_features(
    historical_df,
    snapshot_date="2011-09-10"
)

# 10. Combine RFM Features with Labels


logistic_df = rfm_df.join(
    labeled_customers,
    on="CustomerID",
    how="inner"
)
# 11. Select Required Columns

logistic_df = logistic_df.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary",
    "Label"
)
print("\n===== LOGISTIC REGRESSION DATASET =====")

logistic_df.show(10, truncate=False)

print("\n===== FINAL DATASET SUMMARY =====")

print("Total rows:", logistic_df.count())
print("Total columns:", len(logistic_df.columns))

logistic_df.printSchema()


# 9. Display Results


print("\n===== DATA PERIOD SUMMARY =====")
print("Historical transactions:", historical_df.count())
print("Future transactions:", future_df.count())
print("Historical customers:", historical_customers.count())

print("\n===== CUSTOMER CLASSIFICATION LABELS =====")
labeled_customers.groupBy("Label").agg(
    count("*").alias("Total Customers")
).orderBy("Label").show()
print("\n===== SAMPLE CUSTOMER LABELS =====")
labeled_customers.show(20, truncate=False)

# Save Prepared Dataset
logistic_df.write \
    .mode("overwrite") \
    .parquet("data/processed/logistic_customer_data")

print("\n===== LOGISTIC DATASET SAVED =====")

# 10. Stop Spark
spark.stop()