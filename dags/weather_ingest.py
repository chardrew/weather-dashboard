from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from config.properties import kafka_config as kafka
from plugins.dag_utils import create_spark_task


# Create DAG object
dag = DAG(
    dag_id='WeatherIngest',
    description='Continually ingests real-time weather data from Kafka into PostgreSQL via Spark (Long running)',
    default_args={'owner': 'Chardrew', 'start_date': datetime(2025, 4, 13),},
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
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
ingest = create_spark_task('ingest.py', dag=dag, jars=[dep1, dep2])

# Define task sequence and dependencies
wait_for_kafka_message >> ingest
