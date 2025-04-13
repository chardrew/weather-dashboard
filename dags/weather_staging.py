from datetime import datetime
from airflow import DAG
from plugins.dag_utils import create_spark_task

# Create DAG object
dag = DAG(
    dag_id='WeatherStagingUpdate',
    description='Deduplicates raw weather data and updates staging and cities tables in PostgreSQL',
    default_args={'owner': 'Chardrew', 'start_date': datetime(2025, 4, 13),},
    schedule_interval=None,  # triggered by websocket server once pg_notify detects an update
    catchup=False,
    max_active_runs=1,
)

# List dependencies
dep1 = 'org.postgresql:postgresql:42.6.0'  # PostgreSQL JDBC driver

# Create Spark tasks for Airflow
update_staging = create_spark_task('update_staging.py', dag=dag, jars=[dep1])

# Define task sequence and dependencies
update_staging