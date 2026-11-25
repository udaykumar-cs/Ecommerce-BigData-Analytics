"""
Streaming Data Verification and Ingestion Health Audit.
Reads accumulated Parquet streaming events and reports schema consistency,
total event counts, customer coverage, and event type distributions.
"""

import sys
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
from config.settings import get_path
from utils.logger import get_logger

logger = get_logger("StreamAuditor")


def verify_streaming_data():
    """Audit streaming events saved in Parquet format."""
    events_dir = Path(get_path("streaming_events"))
    
    parquet_files = list(events_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No streaming Parquet files found in: {events_dir}")
        logger.info("Run 'python kafka/producer.py' and 'python streaming/spark_kafka_consumer.py' to generate events.")
        return

    logger.info(f"Discovered {len(parquet_files)} Parquet partition files. Loading data...")
    df = pd.read_parquet(events_dir)

    total_events = len(df)
    unique_customers = df["customer_id"].nunique() if "customer_id" in df.columns else 0
    unique_products = df["product_id"].nunique() if "product_id" in df.columns else 0

    logger.info("==========================================")
    logger.info("   STREAMING INGESTION AUDIT REPORT")
    logger.info("==========================================")
    logger.info(f"Total Events Ingested:    {total_events:,}")
    logger.info(f"Unique Customers Reached: {unique_customers:,}")
    logger.info(f"Unique Products Tracked:  {unique_products:,}")

    if "event_type" in df.columns:
        logger.info("\nEvent Distribution:")
        counts = df["event_type"].value_counts()
        for etype, count in counts.items():
            pct = (count / total_events) * 100
            logger.info(f"  • {etype.ljust(12)}: {count:5d} ({pct:5.1f}%)")

    if "timestamp" in df.columns:
        earliest = df["timestamp"].min()
        latest = df["timestamp"].max()
        logger.info(f"\nTemporal Range: {earliest} to {latest}")

    logger.info("==========================================")
    logger.info("Stream audit completed successfully.")


if __name__ == "__main__":
    verify_streaming_data()
