"""
Comprehensive Unit & Data Quality Invariant Test Suite.
Verifies configuration loading, data cleaning filters, revenue invariants,
RFM feature metrics, and streaming schema conformance.
"""

import unittest
from datetime import datetime
import pandas as pd
from config.settings import get_kafka_config, get_spark_config, get_ml_config, get_path


class TestEcommerceDataPipeline(unittest.TestCase):

    def test_configuration_loading(self):
        """Verify centralized settings load without missing keys."""
        kafka_cfg = get_kafka_config()
        spark_cfg = get_spark_config()
        ml_cfg = get_ml_config()

        self.assertIn("bootstrap_servers", kafka_cfg)
        self.assertIn("topic", kafka_cfg)
        self.assertEqual(kafka_cfg["topic"], "customer-events")

        self.assertIn("app_name", spark_cfg)
        self.assertIn("snapshot_date", ml_cfg)
        self.assertEqual(ml_cfg["kmeans_k"], 3)

        raw_csv_path = get_path("raw_csv")
        self.assertTrue(len(raw_csv_path) > 0)

    def test_data_cleaning_filters(self):
        """Verify that cancelled orders ('C'), negative quantities, and zero unit prices are rejected."""
        sample_data = pd.DataFrame([
            {"InvoiceNo": "536365", "Quantity": 6, "UnitPrice": 2.55, "CustomerID": 17850},   # Valid
            {"InvoiceNo": "C536379", "Quantity": -1, "UnitPrice": 3.75, "CustomerID": 17850},  # Cancelled
            {"InvoiceNo": "A536380", "Quantity": 1, "UnitPrice": 10.0, "CustomerID": 17850},   # Bad debt adjustment
            {"InvoiceNo": "536381", "Quantity": 0, "UnitPrice": 1.25, "CustomerID": 17850},    # Zero quantity
            {"InvoiceNo": "536382", "Quantity": 2, "UnitPrice": -1.00, "CustomerID": 17850},   # Negative price
            {"InvoiceNo": "536383", "Quantity": 4, "UnitPrice": 4.50, "CustomerID": None},     # Missing customer
        ])

        valid = sample_data[
            (sample_data["Quantity"] > 0) &
            (sample_data["UnitPrice"] > 0) &
            (~sample_data["InvoiceNo"].astype(str).str.startswith(("C", "A"))) &
            (sample_data["CustomerID"].notna())
        ].copy()

        self.assertEqual(len(valid), 1)
        self.assertEqual(valid.iloc[0]["InvoiceNo"], "536365")

    def test_revenue_calculation(self):
        """Verify revenue calculation matches unit price * quantity."""
        sample = pd.DataFrame([
            {"Quantity": 10, "UnitPrice": 15.50},
            {"Quantity": 2, "UnitPrice": 50.00}
        ])
        sample["Revenue"] = sample["Quantity"] * sample["UnitPrice"]

        self.assertEqual(sample.loc[0, "Revenue"], 155.0)
        self.assertEqual(sample.loc[1, "Revenue"], 100.0)
        self.assertEqual(sample["Revenue"].sum(), 255.0)

    def test_streaming_event_schema(self):
        """Validate expected keys and constraints in Kafka clickstream event payload."""
        required_keys = {"event_id", "customer_id", "product_id", "event_type", "quantity", "unit_price", "timestamp"}
        valid_event = {
            "event_id": "e82b75a1-d70b-466f-b258-6da83bb9f015",
            "customer_id": 10022,
            "product_id": 1001,
            "event_type": "purchase",
            "quantity": 2,
            "unit_price": 49.99,
            "timestamp": "2026-11-19T13:00:00Z"
        }
        self.assertTrue(required_keys.issubset(valid_event.keys()))
        self.assertIn(valid_event["event_type"], ["view", "add_to_cart", "purchase", "cancel"])
        self.assertGreater(valid_event["unit_price"], 0)

    def test_rfm_calculation_logic(self):
        """Verify RFM feature derivations against snapshot date."""
        snapshot_date = datetime.strptime("2011-12-10", "%Y-%m-%d")
        customer_tx = pd.DataFrame([
            {"CustomerID": 101, "InvoiceNo": "INV1", "InvoiceDate": "2011-12-05", "Revenue": 150.0},
            {"CustomerID": 101, "InvoiceNo": "INV2", "InvoiceDate": "2011-12-08", "Revenue": 250.0},
        ])
        customer_tx["InvoiceDate"] = pd.to_datetime(customer_tx["InvoiceDate"])

        last_purchase = customer_tx["InvoiceDate"].max()
        recency = (snapshot_date - last_purchase).days
        frequency = customer_tx["InvoiceNo"].nunique()
        monetary = customer_tx["Revenue"].sum()

        self.assertEqual(recency, 2)
        self.assertEqual(frequency, 2)
        self.assertEqual(monetary, 400.0)


if __name__ == "__main__":
    unittest.main()
