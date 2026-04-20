from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState


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
    if result.exit_code == 99:
        raise AirflowSkipException(f"Script {script_name} skipped due to no source data")
    if result.exit_code != 0:
        raise RuntimeError(
            f"Script {script_name} failed in pyspark-jupyter with exit code {result.exit_code}"
        )


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="warsaw_buses_1min_aggregations",
    default_args=default_args,
    description="Calculate 1-minute aggregations after realtime pipeline finishes",
    schedule_interval="* * * * *",
    start_date=datetime(2026, 4, 16),
    catchup=False,
    max_active_runs=1,
    tags=["ZTM", "Aggregations", "Spark", "1min"],
) as dag:
    wait_for_realtime = ExternalTaskSensor(
        task_id="wait_for_warsaw_buses_realtime",
        external_dag_id="warsaw_buses_realtime",
        external_task_id="calculate_gold_delays",
        allowed_states=[TaskInstanceState.SUCCESS],
        failed_states=[TaskInstanceState.FAILED, TaskInstanceState.SKIPPED, TaskInstanceState.UPSTREAM_FAILED],
        soft_fail=True,
        mode="reschedule",
        poke_interval=10,
        timeout=300,
    )

    run_stop_aggregations = PythonOperator(
        task_id="spark_calculate_stop_aggregations",
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Gold_Delays_by_Stop.py"},
        priority_weight=30,
    )

    run_district_aggregations = PythonOperator(
        task_id="spark_calculate_district_aggregations",
        python_callable=run_script_in_pyspark,
        op_kwargs={"script_name": "Gold_Delays_by_District.py"},
        priority_weight=30,
    )

    wait_for_realtime >> run_stop_aggregations >> run_district_aggregations
