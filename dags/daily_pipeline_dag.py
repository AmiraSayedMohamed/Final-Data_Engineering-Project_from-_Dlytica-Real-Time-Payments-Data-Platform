from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'dlytica',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_silver_to_gold_and_dbt',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command='docker compose run --rm spark python /app/spark_batch/silver_to_gold.py',
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt',
        bash_command='docker compose run --rm airflow dbt run --profiles-dir /opt/airflow/src/dbt_profiles --project-dir /opt/airflow/src/dbt_project',
    )

    export_reports = BashOperator(
        task_id='export_reports',
        bash_command='docker compose run --rm spark python /app/src/reports/generate_reports.py',
    )

    run_silver_to_gold >> run_dbt_models >> export_reports
