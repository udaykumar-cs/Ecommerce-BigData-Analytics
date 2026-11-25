"""
Production-grade Modular PySpark ETL Pipeline.
Implements Medallion Lakehouse Architecture (Bronze -> Silver -> Gold).

Features:
- Bronze: Ingestion of raw transactional records.
- Silver: Strict data validation, cancellation filtering, temporal extraction, Snappy Parquet partitioning.
- Gold: Dimensional star schema aggregates (dim_customer, dim_product, fact_sales_summary).
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, min as spark_min, max as spark_max,
    countDistinct, to_timestamp, year, month, to_date, lit, datediff
)
from pyspark.ml.feature import VectorAssembler, StandardScaler

from config.settings import get_path, get_spark_config, get_ml_config
from utils.logger import get_logger

logger = get_logger("PySparkETL")


def create_spark_session() -> SparkSession:
    """Initialize a tuned SparkSession with configuration parameters."""
    cfg = get_spark_config()
    logger.info("Initializing SparkSession...")
    spark = (
        SparkSession.builder
        .appName(cfg.get("app_name", "Ecommerce_ETL_Pipeline"))
        .master(cfg.get("master", "local[*]"))
        .config("spark.sql.shuffle.partitions", str(cfg.get("shuffle_partitions", 4)))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(cfg.get("log_level", "WARN"))
    logger.info("SparkSession initialized successfully.")
    return spark


def extract_raw_transactions(spark: SparkSession, file_path: str = None):
    """Bronze Layer: Ingest raw CSV transactions with inferred schema."""
    input_path = file_path or get_path("raw_csv")
    logger.info(f"Extracting raw data from: {input_path}")
    
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )
    total_raw = df.count()
    logger.info(f"Bronze Layer extracted: {total_raw:,} records.")
    return df


def transform_silver_transactions(df):
    """
    Silver Layer: Clean transactions and enforce data invariants.
    - Exclude cancellations ('C' prefix) and debt adjustments ('A' prefix).
    - Exclude zero or negative UnitPrice and Quantity.
    - Enforce timestamp and derive Year, Month, and Revenue columns.
    """
    logger.info("Transforming raw data into Silver conformed layer...")
    
    raw_count = df.count()
    
    # 1. Cleaning filters
    cleaned = df.filter(
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0) &
        (~col("InvoiceNo").startswith("C")) &
        (~col("InvoiceNo").startswith("A")) &
        (col("CustomerID").isNotNull())
    )
    
    # 2. Add derived features and temporal partitions
    cleaned = cleaned.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
    cleaned = cleaned.withColumn("InvoiceTimestamp", to_timestamp(col("InvoiceDate")))
    cleaned = cleaned.withColumn("TransactionDate", to_date(col("InvoiceTimestamp")))
    cleaned = cleaned.withColumn("Year", year(col("InvoiceTimestamp")))
    cleaned = cleaned.withColumn("Month", month(col("InvoiceTimestamp")))
    
    valid_count = cleaned.count()
    filtered_out = raw_count - valid_count
    
    total_revenue = cleaned.select(spark_sum("Revenue")).collect()[0][0] or 0.0
    
    logger.info(f"Silver Transformation Complete:")
    logger.info(f"  • Raw Rows: {raw_count:,}")
    logger.info(f"  • Valid Rows: {valid_count:,}")
    logger.info(f"  • Dropped (Invalid/Cancelled/Null): {filtered_out:,}")
    logger.info(f"  • Net Revenue: £{total_revenue:,.2f}")
    
    return cleaned


def build_gold_dimensions(silver_df):
    """
    Gold Layer: Create dimensional tables for business intelligence and ML.
    - dim_customer: Recency, Frequency, Monetary summary per customer.
    - dim_product: Product catalog aggregation with total volume and revenue.
    - fact_sales_monthly: Aggregated monthly revenue and transactions by country.
    """
    logger.info("Building Gold Layer dimensional tables...")
    ml_cfg = get_ml_config()
    snapshot_date = ml_cfg.get("snapshot_date", "2011-12-10")
    
    # 1. dim_customer (RFM aggregation)
    dim_customer = silver_df.groupBy("CustomerID").agg(
        spark_max("TransactionDate").alias("LastPurchaseDate"),
        countDistinct("InvoiceNo").alias("Frequency"),
        spark_sum("Revenue").alias("Monetary"),
        spark_sum("Quantity").alias("TotalQuantityPurchased"),
        spark_min("Country").alias("PrimaryCountry")
    )
    dim_customer = dim_customer.withColumn(
        "Recency",
        datediff(lit(snapshot_date), col("LastPurchaseDate"))
    )
    
    # 2. dim_product
    dim_product = silver_df.groupBy("StockCode", "Description").agg(
        spark_sum("Quantity").alias("TotalUnitsSold"),
        spark_sum("Revenue").alias("TotalProductRevenue"),
        countDistinct("InvoiceNo").alias("TotalOrdersAppearedIn")
    )
    
    # 3. fact_sales_monthly
    fact_sales_monthly = silver_df.groupBy("Year", "Month", "Country").agg(
        spark_sum("Revenue").alias("MonthlyRevenue"),
        spark_sum("Quantity").alias("MonthlyUnitsSold"),
        countDistinct("InvoiceNo").alias("TotalInvoices")
    )
    
    logger.info("Gold dimensional models built successfully.")
    return dim_customer, dim_product, fact_sales_monthly


def run_etl_pipeline():
    """Main execution entrypoint for the PySpark ETL pipeline."""
    logger.info("=== STARTING E-COMMERCE BATCH ETL PIPELINE ===")
    spark = create_spark_session()
    
    try:
        # Extract Bronze
        raw_df = extract_raw_transactions(spark)
        
        # Transform Silver
        silver_df = transform_silver_transactions(raw_df)
        
        # Save Silver Parquet partitioned by Year and Month
        silver_path = get_path("silver_parquet")
        logger.info(f"Saving Silver Layer to Parquet: {silver_path}")
        silver_df.write.mode("overwrite").partitionBy("Year", "Month").parquet(silver_path)
        logger.info("Silver Parquet saved successfully.")
        
        # Build Gold Models
        dim_cust, dim_prod, fact_monthly = build_gold_dimensions(silver_df)
        
        # Save Gold Summary tables
        gold_cust_path = os.path.join(get_path("processed_dir"), "gold_dim_customer")
        gold_prod_path = os.path.join(get_path("processed_dir"), "gold_dim_product")
        
        dim_cust.write.mode("overwrite").parquet(gold_cust_path)
        dim_prod.write.mode("overwrite").parquet(gold_prod_path)
        
        logger.info("Gold dimensional tables successfully written to Parquet.")
        logger.info("=== E-COMMERCE BATCH ETL PIPELINE COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed with error: {str(e)}", exc_info=True)
        raise e
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    run_etl_pipeline()
