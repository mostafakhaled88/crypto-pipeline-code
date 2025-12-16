# dags/medallion_crypto_dag.py

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

from medallion.bronze_extract import extract_and_load_bronze
from medallion.silver_transform import transform_and_load_silver
from medallion.gold_curation import curate_gold_metrics

log = logging.getLogger(__name__)

@dag(
    dag_id="medallion_crypto_pipeline",
    start_date=datetime(2025, 12, 10),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "data_engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["medallion", "crypto"],
)
def medallion_pipeline():

    @task
    def bronze_task(ds: str):
        """Extracts raw crypto prices into Bronze layer"""
        log.info(f"Running Bronze extraction for date: {ds}")
        extract_and_load_bronze(ds)

    @task
    def silver_task(ds: str):
        """Transforms Bronze data into Silver layer"""
        log.info(f"Running Silver transformation for date: {ds}")
        transform_and_load_silver(ds)

    @task
    def gold_task(ds: str):
        """Aggregates Silver data into Gold metrics"""
        log.info(f"Running Gold curation for date: {ds}")
        curate_gold_metrics(ds)

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    bronze = bronze_task()
    silver = silver_task()
    gold = gold_task()

    bronze >> silver >> gold >> pipeline_complete


dag = medallion_pipeline()
