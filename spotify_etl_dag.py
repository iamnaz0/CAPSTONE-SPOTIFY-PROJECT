from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for the DAG
default_args = {
    'owner': 'spotify_etl_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spotify_etl_daily',                         # DAG ID (shows in UI)
    default_args=default_args,
    description='Run Spotify ETL daily',
    schedule_interval='0 22 * * *',              # Runs daily at 10 PM
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task: Run the ETL Python script inside the DAGs folder
run_etl = BashOperator(
    task_id='run_spotify_etl',
    bash_command='python /opt/airflow/dags/spotify_etl.py',
)