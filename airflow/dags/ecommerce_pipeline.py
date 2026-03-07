from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)   as dag:

    ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command="docker exec ecommerce_ingestion python /app/app/load_data.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="docker exec ecommerce_dbt dbt run --project-dir /usr/app",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="docker exec ecommerce_dbt dbt test --project-dir /usr/app",
    )

    ingestion >> dbt_run >> dbt_test