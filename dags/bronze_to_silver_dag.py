from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'dlytica',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='bronze_to_silver_hourly',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command='docker compose run --rm spark python /app/spark_batch/bronze_to_silver.py',
    )

    run_bronze_to_silver
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bronze_to_silver_hourly',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    run_bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command='python /opt/airflow/src/spark_batch/bronze_to_silver.py'
    )

    run_bronze_to_silver
