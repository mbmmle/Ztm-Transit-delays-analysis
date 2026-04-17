from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_script_in_pyspark(script_name: str) -> None:
    import docker

    client = docker.from_env()
    container = client.containers.get("pyspark-jupyter")
    result = container.exec_run(
        cmd=f"python /home/jovyan/work/scripts/{script_name}",
        stdout=True,
        stderr=True,
    )
    output = (result.output or b"").decode("utf-8", errors="replace")
    if output:
        print(output)
    if result.exit_code != 0:
        raise RuntimeError(
            f"Script {script_name} failed in pyspark-jupyter with exit code {result.exit_code}"
        )

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='warsaw_buses_daily_master',
    default_args=default_args,
    description='Builds master schedule',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 4, 16),
    catchup=False,
    tags=['ZTM', 'Batch', 'Spark']
) as dag:
    build_bronze_layer = PythonOperator(
        task_id='run_spark_bronze_gtfs',
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Bronze_GTFS.py"},
    )

    build_silver_layer = PythonOperator(
        task_id='run_spark_silver_gtfs',
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Silver_GTFS.py"},
    )

    build_schedule = PythonOperator(
        task_id='run_spark_master_schedule',
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Silver_Master_Schedule.py"},
    )

    build_bronze_layer >> build_silver_layer >> build_schedule