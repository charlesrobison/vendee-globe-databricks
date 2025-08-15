# airflow/dags/vendee_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fetch_vendee_data import fetch_vendee_data

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="vendee_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_vendee_data",
        python_callable=fetch_vendee_data
    )

    fetch_task
