from datetime import datetime
from airflow import DAG
from plugins.dag_utils import create_spark_task

# Create DAG object
dag = DAG(
    dag_id='WeatherTransform',
    description='Performs hourly aggregations on staging data and updates the aggregate table for dashboard visualisation',
    default_args={'owner': 'Chardrew', 'start_date': datetime(2025, 4, 13),},
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
)

# List dependencies
dep1 = 'org.postgresql:postgresql:42.6.0'  # PostgreSQL JDBC driver

# Create Spark tasks for Airflow
transform = create_spark_task('transform.py', dag=dag, jars=[dep1])

# Define task sequence and dependencies
transform
