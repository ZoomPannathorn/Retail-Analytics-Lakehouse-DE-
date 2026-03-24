from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract.generate_raw_data import main as generate_raw_data
from src.load.load_to_postgres import run as load_to_postgres
from src.quality.checks import run_checks
from src.transform.build_silver import run as build_silver


with DAG(
    dag_id="ecommerce_analytics_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "data-engineering", "analytics"],
) as dag:
    ingest = PythonOperator(task_id="generate_raw_data", python_callable=generate_raw_data)
    transform = PythonOperator(task_id="build_silver_layer", python_callable=build_silver)
    quality = PythonOperator(task_id="run_quality_checks", python_callable=run_checks)
    load = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    ingest >> transform >> quality >> load
