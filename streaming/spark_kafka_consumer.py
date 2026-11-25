"""
Real-time Spark Structured Streaming Consumer.
Consumes JSON clickstream events from Apache Kafka, parses schemas,
applies watermarking, and writes to partitioned Parquet with fault-tolerant checkpointing.
"""

import sys
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)

from config.settings import get_kafka_config, get_path, get_spark_config
from utils.logger import get_logger

logger = get_logger("SparkStreamingConsumer")

# Explicit Kafka Event Schema Definition
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


def run_streaming_consumer():
    """Start the Spark Structured Streaming consumer pipeline."""
    kafka_cfg = get_kafka_config()
    spark_cfg = get_spark_config()
    
    bootstrap_servers = ",".join(kafka_cfg.get("bootstrap_servers", ["localhost:9092"]))
    topic = kafka_cfg.get("topic", "customer-events")
    output_path = get_path("streaming_events")
    checkpoint_path = get_path("streaming_checkpoint")
    trigger_seconds = spark_cfg.get("streaming_trigger_seconds", 5)

    logger.info("Initializing Spark Structured Streaming consumer...")
    logger.info(f"Target Kafka Topic: '{topic}' at {bootstrap_servers}")
    logger.info(f"Parquet Output Destination: {output_path}")
    logger.info(f"Checkpoint Metadata Location: {checkpoint_path}")

    spark = (
        SparkSession.builder
        .appName("Ecommerce_Kafka_Streaming_Consumer")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Ingest raw streaming messages from Kafka
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # 2. Deserialize JSON payloads and cast timestamps
        parsed_events = (
            kafka_stream
            .selectExpr("CAST(value AS STRING) AS json_payload")
            .select(from_json(col("json_payload"), EVENT_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("EventTime", to_timestamp(col("timestamp")))
        )

        # 3. Apply Watermarking for late-arriving event tolerance (10 min watermark)
        watermarked_events = (
            parsed_events
            .withWatermark("EventTime", "10 minutes")
        )

        logger.info("Starting streaming write to Parquet with write-ahead checkpointing...")

        # 4. Write stream to Parquet files
        query = (
            watermarked_events.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )

        logger.info(f"Consumer active. Listening for events on topic '{topic}'...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.warning("Streaming consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Streaming consumer failed: {str(e)}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session terminated.")


if __name__ == "__main__":
    run_streaming_consumer()
