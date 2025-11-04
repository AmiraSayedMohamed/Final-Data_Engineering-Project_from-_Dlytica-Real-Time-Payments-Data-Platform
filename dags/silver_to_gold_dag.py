from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='silver_to_gold_daily',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command='python /opt/airflow/src/spark_batch/silver_to_gold.py'
    )

    # example dbt run (assuming dbt is available in the airflow environment)
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt_project && dbt deps && dbt run && dbt test'
    )

    run_export = BashOperator(
        task_id='export_reports',
        bash_command='python /opt/airflow/scripts/export_reports.py'
    )

    run_tests = BashOperator(
        task_id='run_integration_tests',
        bash_command='cd /opt/airflow && pytest -q tests/test_integration.py'
    )

    run_silver_to_gold >> run_dbt >> run_export >> run_tests
