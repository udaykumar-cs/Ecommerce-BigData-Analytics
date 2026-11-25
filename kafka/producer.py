"""
Real-time E-Commerce Clickstream Event Producer.
Publishes synthetic customer actions (view, add_to_cart, purchase, cancel) to Apache Kafka.
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config.settings import get_kafka_config
from utils.logger import get_logger

logger = get_logger("KafkaProducer")


def create_producer(bootstrap_servers: list) -> KafkaProducer:
    """Connect to Kafka broker with JSON serializer."""
    logger.info(f"Connecting to Kafka brokers at: {bootstrap_servers}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda val: json.dumps(val).encode("utf-8"),
            retries=3
        )
        logger.info("Successfully connected to Kafka broker.")
        return producer
    except NoBrokersAvailable:
        logger.error(
            "Could not connect to Kafka broker. "
            "Please ensure Kafka is running (e.g., via 'docker compose up -d')."
        )
        sys.exit(1)


def generate_event(event_types: list) -> dict:
    """Generate a realistic randomized retail clickstream event."""
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": random.randint(10001, 10050),
        "product_id": random.randint(1001, 1020),
        "event_type": random.choice(event_types),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def run_producer(num_events: int = 30, delay: float = 0.5, continuous: bool = False):
    """Publish simulated events to Kafka topic."""
    kafka_cfg = get_kafka_config()
    servers = kafka_cfg.get("bootstrap_servers", ["localhost:9092"])
    topic = kafka_cfg.get("topic", "customer-events")
    event_types = kafka_cfg.get("event_types", ["view", "add_to_cart", "purchase", "cancel"])

    producer = create_producer(servers)
    logger.info(f"Starting event stream into topic: '{topic}' (Target events: {num_events if not continuous else 'Continuous'})...")

    count = 0
    try:
        while continuous or (count < num_events):
            event = generate_event(event_types)
            producer.send(topic, value=event)
            count += 1
            logger.info(f"[{count:04d}] Sent {event['event_type'].upper()} event: Cust={event['customer_id']} Prod={event['product_id']} (£{event['unit_price']})")
            time.sleep(delay)

        producer.flush()
        logger.info(f"Successfully published {count} events to topic '{topic}'.")

    except KeyboardInterrupt:
        logger.warning("Producer execution interrupted by user.")
    finally:
        producer.close()
        logger.info("Kafka producer connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Real-Time Kafka Event Producer")
    parser.add_argument("--events", type=int, default=30, help="Number of events to publish (default: 30)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay in seconds between events (default: 0.5)")
    parser.add_argument("--continuous", action="store_true", help="Publish continuously until stopped")

    args = parser.parse_args()
    run_producer(num_events=args.events, delay=args.delay, continuous=args.continuous)
