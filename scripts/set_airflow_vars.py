from airflow.models import Variable
from airflow.utils.db import provide_session

@provide_session
def set_airflow_variables(session=None):  # TODO substitute for docker secrets
    # OpenWeather API
    Variable.set('OPENWEATHER_API_KEY', '749d618f22cfb4068129e87d29849d17', session=session)
    Variable.set('DATABASE_USER', 'postgres', session=session)
    Variable.set('DATABASE_PASSWORD', 'dbdemon', session=session)

set_airflow_variables()
