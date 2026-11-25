"""
Centralized settings and configuration loader for the E-Commerce platform.
Resolves relative paths to project-root absolute paths dynamically.
"""

import os
from pathlib import Path

# Project root directory (parent of config folder)
ROOT_DIR = Path(__file__).resolve().parent.parent

# Default configuration dictionary (fallback if YAML is missing)
DEFAULT_CONFIG = {
    "environment": "development",
    "paths": {
        "raw_excel": "data/raw/Online Retail.xlsx",
        "raw_csv": "data/processed/online_retail.csv",
        "processed_dir": "data/processed",
        "silver_parquet": "data/processed/silver_transactions",
        "linear_data": "data/processed/linear_customer_data",
        "logistic_data": "data/processed/logistic_customer_data",
        "streaming_events": "data/streaming/events",
        "streaming_checkpoint": "data/streaming/checkpoint",
        "results_dir": "results"
    },
    "kafka": {
        "bootstrap_servers": ["localhost:9092"],
        "topic": "customer-events",
        "event_types": ["view", "add_to_cart", "purchase", "cancel"],
        "client_id": "ecommerce-analytics-producer"
    },
    "spark": {
        "app_name": "Ecommerce_BigData_Platform",
        "master": "local[*]",
        "log_level": "WARN",
        "shuffle_partitions": 4,
        "streaming_trigger_seconds": 5
    },
    "ml": {
        "snapshot_date": "2011-12-10",
        "cutoff_date": "2011-09-10",
        "future_end_date": "2011-12-09",
        "kmeans_k": 3,
        "random_seed": 42
    }
}

def load_config() -> dict:
    """Load configuration from config.yaml or return DEFAULT_CONFIG."""
    config_path = ROOT_DIR / "config" / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                if loaded:
                    return loaded
        except ImportError:
            pass
    return DEFAULT_CONFIG

CONFIG = load_config()

def get_path(path_key: str) -> str:
    """Retrieve an absolute filesystem path corresponding to a path key in config."""
    rel_path = CONFIG.get("paths", {}).get(path_key, "")
    if not rel_path:
        raise KeyError(f"Path key '{path_key}' not found in configuration.")
    return str((ROOT_DIR / rel_path).resolve())

def get_kafka_config() -> dict:
    """Retrieve Kafka configuration settings."""
    return CONFIG.get("kafka", DEFAULT_CONFIG["kafka"])

def get_spark_config() -> dict:
    """Retrieve Spark configuration settings."""
    return CONFIG.get("spark", DEFAULT_CONFIG["spark"])

def get_ml_config() -> dict:
    """Retrieve Machine Learning parameters."""
    return CONFIG.get("ml", DEFAULT_CONFIG["ml"])
