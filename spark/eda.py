from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import (
    col, sum as spark_sum, min, max, countDistinct,
    datediff, lit
)
# 1. Create SparkSession
spark = SparkSession.builder \
    .appName("Ecommerce_EDA") \
    .master("local[*]") \
    .getOrCreate()

print("\n===== SPARK SESSION CREATED =====")

# 2. Read CSV using PySpark
df = spark.read.csv(
    "data/processed/online_retail.csv",
    header=True,
    inferSchema=True
)

# 3. Display first 5 records
print("\n===== FIRST 5 RECORDS =====")
df.show(5, truncate=False)

# 4. Display total rows and columns
print("\n===== DATASET DIMENSIONS =====")
print("Rows:", df.count())
print("Columns:", len(df.columns))

# 5. Display column names
print("\n===== COLUMN NAMES =====")
print(df.columns)

# 6. Display schema
print("\n===== DATASET SCHEMA =====")
df.printSchema()

# 8. Missing Value Analysis
print("\n===== MISSING VALUE ANALYSIS =====")

missing_counts = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

missing_counts.show(truncate=False)

# 9. Duplicate Records Analysis
print("\n===== DUPLICATE RECORD ANALYSIS =====")

total_rows = df.count()

unique_rows = df.dropDuplicates().count()

duplicate_rows = total_rows - unique_rows

print("Total Records:", total_rows)
print("Unique Records:", unique_rows)
print("Duplicate Records:", duplicate_rows)

# 10. Negative and Zero Value Analysis

print("\n===== NEGATIVE AND ZERO VALUE ANALYSIS =====")

# Negative quantities
negative_quantity = df.filter(col("Quantity") < 0).count()

# Zero quantities
zero_quantity = df.filter(col("Quantity") == 0).count()

# Negative unit prices
negative_price = df.filter(col("UnitPrice") < 0).count()

# Zero unit prices
zero_price = df.filter(col("UnitPrice") == 0).count()

print("Negative Quantity Records:", negative_quantity)
print("Zero Quantity Records:", zero_quantity)
print("Negative UnitPrice Records:", negative_price)
print("Zero UnitPrice Records:", zero_price)


# 11. Cancellation Invoice Analysis

print("\n===== CANCELLATION ANALYSIS =====")

cancellation_records = df.filter(
    col("InvoiceNo").startswith("C")
)

print("Cancellation Records:", cancellation_records.count())

print("Sample Cancellation Records:")
cancellation_records.show(5, truncate=False)

# 12. Investigate Unusual Records

print("\n===== NEGATIVE UNIT PRICE RECORDS =====")

df.filter(col("UnitPrice") < 0).show(
    truncate=False
)

print("\n===== ZERO UNIT PRICE SAMPLE =====")

df.filter(col("UnitPrice") == 0).show(
    10, truncate=False
)

print("\n===== NEGATIVE QUANTITY WITHOUT CANCELLATION =====")

df.filter(
    (col("Quantity") < 0) &
    (~col("InvoiceNo").startswith("C"))
).show(10, truncate=False)

print("\n===== CANCELLATION WITH POSITIVE QUANTITY =====")

df.filter(
    (col("InvoiceNo").startswith("C")) &
    (col("Quantity") > 0)
).count()


# 13. Date Range Analysis

print("\n===== DATE RANGE ANALYSIS =====")

df.select(
    min("InvoiceDate").alias("First Transaction"),
    max("InvoiceDate").alias("Last Transaction")
).show(truncate=False)


# 14. Sales Statistics

print("\n===== SALES STATISTICS =====")

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

sales_df.select(
    spark_sum("Quantity").alias("Total Quantity Sold"),
    spark_sum("Revenue").alias("Total Revenue"),
    countDistinct("CustomerID").alias("Unique Customers"),
    countDistinct("StockCode").alias("Unique Products")
).show(truncate=False)

# 15. Country-wise Sales Analysis

print("\n===== COUNTRY-WISE SALES ANALYSIS =====")

country_sales = sales_df.groupBy("Country").agg(
    spark_sum("Quantity").alias("Total Quantity"),
    spark_sum("Revenue").alias("Total Revenue"),
    countDistinct("InvoiceNo").alias("Total Invoices")
)

country_sales = country_sales.orderBy(
    col("Total Revenue").desc()
)

print("\n===== TOP 10 COUNTRIES BY REVENUE =====")

country_sales.show(10, truncate=False)


# 16. Product-wise Sales Analysis

print("\n===== PRODUCT-WISE SALES ANALYSIS =====")

product_sales = sales_df.groupBy(
    "StockCode", "Description"
).agg(
    spark_sum("Quantity").alias("Total Quantity"),
    spark_sum("Revenue").alias("Total Revenue")
)

# Top 10 products by quantity
print("\n===== TOP 10 PRODUCTS BY QUANTITY =====")

product_sales.orderBy(
    col("Total Quantity").desc()
).show(10, truncate=False)


# Top 10 products by revenue
print("\n===== TOP 10 PRODUCTS BY REVENUE =====")

product_sales.orderBy(
    col("Total Revenue").desc()
).show(10, truncate=False)


# 17. Customer Behaviour Analysis

print("\n===== CUSTOMER BEHAVIOUR ANALYSIS =====")

# Exclude records without CustomerID
customer_df = sales_df.filter(
    col("CustomerID").isNotNull()
)

customer_sales = customer_df.groupBy(
    "CustomerID"
).agg(
    spark_sum("Revenue").alias("Total Spending"),
    spark_sum("Quantity").alias("Total Quantity"),
    countDistinct("InvoiceNo").alias("Purchase Frequency")
)

print("\n===== TOP 10 CUSTOMERS BY SPENDING =====")

customer_sales.orderBy(
    col("Total Spending").desc()
).show(10, truncate=False)


# 18. RFM Analysis

print("\n===== RFM ANALYSIS =====")

# Snapshot date: one day after the last transaction
snapshot_date = "2011-12-10"

rfm_df = customer_df.groupBy("CustomerID").agg(

    # Recency: Last purchase date
    max("InvoiceDate").alias("Last Purchase Date"),

    # Frequency: Number of distinct invoices
    countDistinct("InvoiceNo").alias("Frequency"),

    # Monetary: Total spending
    spark_sum("Revenue").alias("Monetary")
)

# Calculate Recency in days
rfm_df = rfm_df.withColumn(
    "Recency",
    datediff(
        lit(snapshot_date),
        col("Last Purchase Date")
    )
)

print("\n===== SAMPLE RFM RECORDS =====")

rfm_df.select(
    "CustomerID",
    "Recency",
    "Frequency",
    "Monetary"
).show(10, truncate=False)

print("\n===== RFM STATISTICS =====")

rfm_df.select(
    "Recency",
    "Frequency",
    "Monetary"
).describe().show(truncate=False)

# 19. Prepare Features for Machine Learning

print("\n===== PREPARING RFM FEATURES =====")

assembler = VectorAssembler(
    inputCols=["Recency", "Frequency", "Monetary"],
    outputCol="Features"
)

rfm_vector_df = assembler.transform(rfm_df)

print("\n===== RFM FEATURE VECTORS =====")

rfm_vector_df.select(
    "CustomerID",
    "Features"
).show(10, truncate=False)

# 20. Standardize RFM Features

print("\n===== STANDARDIZING RFM FEATURES =====")

scaler = StandardScaler(
    inputCol="Features",
    outputCol="ScaledFeatures",
    withStd=True,
    withMean=True
)

scaler_model = scaler.fit(rfm_vector_df)

rfm_scaled_df = scaler_model.transform(rfm_vector_df)

print("\n===== SCALED RFM FEATURES =====")

rfm_scaled_df.select(
    "CustomerID",
    "Features",
    "ScaledFeatures"
).show(10, truncate=False)


# $env:PYTHONPATH = "C:\spark\python\lib\pyspark.zip;C:\spark\python\lib\py4j-0.10.9.7-src.zip"
# 7. Stop Spark
spark.stop()
