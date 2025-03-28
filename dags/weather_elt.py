from pathlib import Path
import airflow
from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.properties import kafka_config as kafka, spark_config as spark

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
        conf={"spark.jars.packages": jars},
        dag=dag,
    )

def log_dag_trigger(message):
    """Kafka message processing function"""
    print(f'🔔 Received Kafka message: DAG triggered!')
    return True

# Create DAG object
dag = DAG(
    dag_id='WeatherELT',
    default_args={'owner': 'Chardrew', 'start_date': airflow.utils.dates.days_ago(1)},
    schedule_interval=None,
    catchup=False,
)

# List dependencies
dep1 = 'org.postgresql:postgresql:42.6.0'  # PostgreSQL JDBC driver
dep2 = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5'  # Spark-Kafka integration

# Create Kafka sensor
wait_for_kafka_message = AwaitMessageSensor(
    task_id="wait_for_kafka_message",
    topics=[kafka.topic],
    kafka_config_id=kafka.connection_id,
    apply_function='utils.kafka_functions.log_dag_trigger',
    poll_timeout=60*60,  # 1 hour
    poll_interval=5,
    dag=dag,
)

# Create Spark tasks for Airflow
kafka_to_db = create_spark_task('kafka_to_db.py', jars=[dep1, dep2])
transform = create_spark_task('transform.py', jars=[dep1])

# Define task sequence and dependencies
wait_for_kafka_message >> kafka_to_db >> transform
