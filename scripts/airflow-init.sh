#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero statusqqqwq


echo "🚀 Running Airflow Initialization..."
echo "🌎 Environment Variables:"
env  # Print environment variables for debugging

echo "📂 PYTHONPATH is set to: $PYTHONPATH"

# Ensure database migrations are applied
echo "🔄 Running Airflow DB Migrate..."
airflow db migrate

echo "✅ Checking Airflow DB..."
airflow db check

# Run initialization scripts
echo "⚙️ Setting Airflow Variables..."
python /opt/airflow/scripts/set_airflow_vars.py

echo "⚙️ Setting Airflow Connections..."
python /opt/airflow/scripts/set_airflow_conns.py

echo "✅ Airflow initialization complete!"
