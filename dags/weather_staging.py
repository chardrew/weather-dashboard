from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.properties import spark_config as spark

def create_task_id(script_name: str):
    """Creates a task ID from a script filename."""
    return f'task-{Path(script_name).stem}'  # e.g., my_script.py → task-my_script


def create_spark_task(script_name: str, jars=None):
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

# Create DAG object
dag = DAG(
    dag_id='WeatherStagingUpdate',
    default_args={'owner': 'Chardrew', 'start_date': datetime(2025, 4, 13),},
    schedule_interval=None,  # triggered by websocket server once pg_notify detects an update
    catchup=False,    

)

# List dependencies
dep1 = 'org.postgresql:postgresql:42.6.0'  # PostgreSQL JDBC driver

# Create Spark tasks for Airflow
update_staging = create_spark_task('update_staging.py', jars=[dep1])

# Define task sequence and dependencies
update_staging