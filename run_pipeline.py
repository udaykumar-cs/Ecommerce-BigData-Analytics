"""
Master Command-Line Interface (CLI) Pipeline Runner.
Provides a unified entry point to execute and orchestrate any stage of the platform.

Usage Examples:
  python run_pipeline.py --step test
  python run_pipeline.py --step verify-stream
  python run_pipeline.py --step producer --events 50
  python run_pipeline.py --step graph
  python run_pipeline.py --step dashboard
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.logger import get_logger

logger = get_logger("PipelineRunner")


def execute_cmd(command: list, description: str):
    """Execute a subprocess command with structured logging."""
    logger.info(f"==> Starting: {description}")
    logger.info(f"Command: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True)
        logger.info(f"==> Finished successfully: {description}\n")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}: {description}")
        return e.returncode
    except KeyboardInterrupt:
        logger.warning(f"Process interrupted: {description}")
        return 130


def run_pipeline():
    parser = argparse.ArgumentParser(
        description="E-Commerce Big Data & Analytics Platform CLI Orchestrator"
    )
    parser.add_argument(
        "--step",
        choices=[
            "test",
            "etl",
            "producer",
            "consumer",
            "verify-stream",
            "graph",
            "dashboard",
            "all"
        ],
        default="test",
        help="Pipeline step to execute (default: test)"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=30,
        help="Number of events for Kafka producer (default: 30)"
    )

    args = parser.parse_args()

    python_bin = sys.executable

    if args.step == "test":
        execute_cmd([python_bin, "-m", "unittest", "discover", "-s", "tests"], "Automated Unit & Quality Tests")

    elif args.step == "etl":
        execute_cmd([python_bin, "spark/etl_pipeline.py"], "PySpark Batch ETL Lakehouse Pipeline")

    elif args.step == "producer":
        execute_cmd(
            [python_bin, "kafka/producer.py", "--events", str(args.events)],
            f"Kafka Event Producer ({args.events} events)"
        )

    elif args.step == "consumer":
        execute_cmd([python_bin, "streaming/spark_kafka_consumer.py"], "Spark Structured Streaming Consumer")

    elif args.step == "verify-stream":
        execute_cmd([python_bin, "streaming/verify_stream.py"], "Streaming Ingestion Health Audit")

    elif args.step == "graph":
        execute_cmd([python_bin, "graph/customer_product_graph.py"], "Customer-Product Bipartite Graph Analytics")

    elif args.step == "dashboard":
        execute_cmd(["streamlit", "run", "dashboard/app.py"], "Streamlit Executive Dashboard")

    elif args.step == "all":
        logger.info("Executing comprehensive pipeline sequence...")
        execute_cmd([python_bin, "-m", "unittest", "discover", "-s", "tests"], "1. Quality & Schema Tests")
        execute_cmd([python_bin, "streaming/verify_stream.py"], "2. Stream Health Audit")
        execute_cmd([python_bin, "graph/customer_product_graph.py"], "3. Network Graph Generation")
        logger.info("Pipeline sequence completed.")


if __name__ == "__main__":
    run_pipeline()
