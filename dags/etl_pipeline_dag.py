from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.etl.jobs.run_extract import run_extract_pipeline
from src.etl.jobs.run_transform import run_transform_pipeline
from src.etl.jobs.run_load import run_load_pipeline


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": int(os.getenv("AIRFLOW_RETRIES", "2")),
    "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "2"))),
}


with DAG(
    dag_id="industrial_etl_books_pipeline",
    description="Pipeline ETL industrialise: extraction, transformation Spark, chargement MinIO",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_multi_sources",
        python_callable=run_extract_pipeline,
    )

    transform_task = PythonOperator(
        task_id="spark_transform",
        python_callable=run_transform_pipeline,
    )

    load_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=run_load_pipeline,
    )

    extract_task >> transform_task >> load_task

