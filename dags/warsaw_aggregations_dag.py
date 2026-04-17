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
    dag_id='warsaw_buses_5min_aggregations',
    default_args=default_args,
    description='Grupuje dane z warstwy Gold dla dzielnic i przystanków',
    schedule_interval='*/5 * * * *', 
    start_date=datetime(2026, 4, 16),
    catchup=False,
    max_active_runs=1,
    tags=['ZTM', 'Aggregations', 'Spark']
) as dag:
    
    run_stop_aggregations = PythonOperator(
        task_id='spark_calculate_stop_aggregations',
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Gold_Delays_by_Stop.py"},
    )
    run_district_aggregations = PythonOperator(
        task_id='spark_calculate_district_aggregations',
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Gold_Delays_by_District.py"},
    )  

    run_stop_aggregations >> run_district_aggregations