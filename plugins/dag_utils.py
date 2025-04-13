from pathlib import Path
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.properties import spark_config as spark


def create_task_id(script_name: str):
    """Creates a task ID from a script filename."""
    return f'task-{Path(script_name).stem}'  # e.g., my_script.py → task-my_script


def create_spark_task(script_name: str, dag, jars=None):
    """Creates a SparkSubmitOperator task in Airflow."""
    task_id = create_task_id(script_name)
    jars = ','.join(jars) if jars else ''
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=spark.connection_id,
        application=f'jobs/python/{script_name}',
        conf={"spark.jars.packages": jars,
              "spark.dynamicAllocation.enabled": spark.dynamic_allocation_enabled,
              "spark.dynamicAllocation.minExecutors": spark.executor_instances_min,
              "spark.dynamicAllocation.maxExecutors": spark.executor_instances_max,
              "spark.executor.cores": spark.executor_cores,
              "spark.executor.memory": spark.executor_memory,
              "spark.driver.cores": spark.driver_cores,
              "spark.driver.memory": spark.driver_memory,
        },
        dag=dag,
    )
