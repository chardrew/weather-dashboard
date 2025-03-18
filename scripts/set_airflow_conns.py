import json, sys
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow import settings

sys.path.append("/opt/airflow/config")  # Ensure Python sees the config folder

print("🛠 Checking if config module is found...")
print("Current sys.path:", sys.path)

try:
    from config.properties import db_config as db, kafka_config as kafka, spark_config as spark
    print("✅ Successfully imported config.properties!")
except ModuleNotFoundError as e:
    print("❌ ModuleNotFoundError:", e)


from config.properties import db_config as db, kafka_config as kafka, spark_config as spark

@provide_session
def add_airflow_connection(conn_id, conn_type, host, port=None, username=None, password=None,
                           schema=None, overwrite=False, session=None, extra=None):
    """Adds or overwrites a new Airflow connection"""

    session = settings.Session()

    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        if overwrite:
            print(f'Removing existing connection: {conn_id}')
            session.delete(existing_conn)
            session.commit()
        else:
            print(f'Connection {conn_id} already exists, skipping...')
            session.close()
            return

    # Create a new connection
    new_conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port,
        login=username,
        password=password,
        schema=schema,
        extra=extra
    )

    session.add(new_conn)
    session.commit()
    session.close()
    print(f'Successfully added connection: {conn_id}')


if __name__ == '__main__':
    # Add Kafka connection
    kafka_extra = json.loads(kafka.connection_extra)
    add_airflow_connection(
        conn_id=kafka.connection_id,
        conn_type=kafka.connection_type,
        host=kafka.host,
        port=kafka.port,
        overwrite=True,
        extra=kafka_extra,
    )

    # Add database connection to store Kafka stream data
    add_airflow_connection(
        conn_id=db.connection_id,
        conn_type=db.connection_type,
        username=db.user,
        password=db.password,
        host=db.host,
        port=db.port,
        overwrite=True
    )

    # Add Spark connection
    add_airflow_connection(
        conn_id=spark.connection_id,
        conn_type=spark.connection_type,
        host=f"{spark.user}://{spark.host}",  # No port included
        port=spark.port,
        overwrite=True,
    )