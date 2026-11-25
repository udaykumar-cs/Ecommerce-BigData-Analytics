"""
Apache Airflow DAG: E-Commerce Lakehouse ETL & Intelligence Pipeline.
Defines workflow dependencies, retry policies, and automated daily scheduling.
"""

from datetime import datetime, timedelta
import os

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Default DAG execution arguments
DEFAULT_ARGS = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="ecommerce_bigdata_lakehouse_pipeline",
        default_args=DEFAULT_ARGS,
        description="Daily Lakehouse ETL, Data Quality, RFM Segmentation & Predictive Training",
        schedule_interval="@daily",
        catchup=False,
        tags=["ecommerce", "pyspark", "etl", "lakehouse"]
    ) as dag:

        start_pipeline = EmptyOperator(task_id="start_pipeline")

        # Task 1: Check source data availability
        check_source_data = BashOperator(
            task_id="check_source_data",
            bash_command="python -c \"import os; assert os.path.exists('data/processed/online_retail.csv'), 'Source data missing'\""
        )

        # Task 2: Execute PySpark Bronze to Silver & Gold ETL
        run_pyspark_etl = BashOperator(
            task_id="run_pyspark_etl",
            bash_command="python spark/etl_pipeline.py"
        )

        # Task 3: Run Data Quality Invariant Assertions
        run_data_quality_tests = BashOperator(
            task_id="run_data_quality_tests",
            bash_command="python -m unittest discover -s tests"
        )

        # Task 4: RFM Customer Segmentation
        run_customer_clustering = BashOperator(
            task_id="run_customer_clustering",
            bash_command="python ml/hierarchical_clustering.py"
        )

        # Task 5: Predictive Modeling (Churn & CLV)
        run_predictive_modeling = BashOperator(
            task_id="run_predictive_modeling",
            bash_command="python ml/logistic_regression.py && python ml/linear_regression.py"
        )

        # Task 6: Bipartite Graph Network Generation
        build_network_graph = BashOperator(
            task_id="build_network_graph",
            bash_command="python graph/customer_product_graph.py"
        )

        # Task 7: Streaming Health Audit
        audit_streaming_health = BashOperator(
            task_id="audit_streaming_health",
            bash_command="python streaming/verify_stream.py"
        )

        end_pipeline = EmptyOperator(task_id="end_pipeline")

        # Dependency Graph
        (
            start_pipeline
            >> check_source_data
            >> run_pyspark_etl
            >> run_data_quality_tests
            >> [run_customer_clustering, run_predictive_modeling, build_network_graph]
            >> audit_streaming_health
            >> end_pipeline
        )
else:
    # Standalone placeholder for environments without Airflow installed
    dag = None
