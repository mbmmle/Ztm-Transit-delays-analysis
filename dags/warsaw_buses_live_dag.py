from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(seconds=20),
}

with DAG(
    dag_id='warsaw_buses_realtime',
    default_args=default_args,
    description='Complete pipeline for processing Warsaw bus GTFS-RT data',
    schedule_interval='* * * * *', 
    start_date=datetime(2026, 4, 16), 
    catchup=False, 
    max_active_runs=1,
    tags=['ZTM', 'Real-Time']
) as dag:


    fetch_rt = BashOperator(
        task_id='fetch_gtfs_rt',
        bash_command='python3 /opt/airflow/scripts/Silver_Bus_Live_Feed.py',
        skip_on_exit_code=99
    )

    match_silver = BashOperator(
        task_id='match_silver_layer',
        bash_command='python3 /opt/airflow/scripts/Silver_Bus_Matched.py'
    )

    calc_gold = BashOperator(
        task_id='calculate_gold_delays',
        bash_command='python3 /opt/airflow/scripts/Gold_Bus_Delays.py'
    )


    fetch_rt >> match_silver >> calc_gold