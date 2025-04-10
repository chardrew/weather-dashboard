import os
from dotenv import load_dotenv
from pydantic import BaseModel

# Load .env file
load_dotenv()


### **Database Configuration**
class DatabaseConfig(BaseModel):
    version: str = os.getenv("DATABASE_VERSION")
    connection_id: str = os.getenv("DATABASE_CONNECTION_ID")
    connection_type: str = os.getenv("DATABASE_CONNECTION_TYPE")
    name: str = os.getenv("DATABASE_NAME")
    table_raw: str = os.getenv("TABLE_NAME_RAW")
    table_agg: str = os.getenv("TABLE_NAME_AGG")
    table_staging: str = os.getenv("TABLE_NAME_STAGING")
    table_cities: str = os.getenv("TABLE_NAME_CITIES")
    user: str = os.getenv("DATABASE_USER")
    password: str = os.getenv("DATABASE_PASSWORD")
    host: str = os.getenv("DATABASE_HOST")
    port: str = os.getenv("DATABASE_PORT")  # Keep as string
    driver: str = os.getenv("DATABASE_DRIVER")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.name}"

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


### **OpenWeather API Configuration**
class OpenWeatherConfig(BaseModel):
    api_url: str = os.getenv("API_URL")
    api_key: str = os.getenv("API_KEY")
    city: str = os.getenv("CITY")
    country: str = os.getenv("COUNTRY")


### **Kafka Configuration**
class KafkaConfig(BaseModel):
    version: str = os.getenv("KAFKA_VERSION")
    topic: str = os.getenv("KAFKA_TOPIC_NAME")
    host: str = os.getenv("KAFKA_HOST_APP")
    port: str = os.getenv("KAFKA_PORT_APP")  # Keep as string
    connection_id: str = os.getenv("KAFKA_CONNECTION_ID")
    connection_type: str = os.getenv("KAFKA_CONNECTION_TYPE")
    connection_extra: str = os.getenv("KAFKA_CONNECTION_EXTRA")
    broker_id: str = os.getenv("KAFKA_BROKER_ID")
    offsets_topic_replication_factor: str = os.getenv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR")

    @property
    def bootstrap_servers(self) -> str:
        return f"{self.host}:{self.port}"


### **Spark Configuration**
class SparkConfig(BaseModel):
    version: str = os.getenv("SPARK_VERSION")
    connection_id: str = os.getenv("SPARK_CONNECTION_ID")
    connection_type: str = os.getenv("SPARK_CONNECTION_TYPE")
    user: str = os.getenv("SPARK_USER")
    host: str = os.getenv("SPARK_HOST")
    port: str = os.getenv("SPARK_PORT")  # Keep as string
    url: str = os.getenv("SPARK_URL")
    master_web_ui_port: str = os.getenv("SPARK_MASTER_WEB_UI_PORT")  # Keep as string
    worker_cores: str = os.getenv("SPARK_WORKER_CORES")
    worker_memory: str = os.getenv("SPARK_WORKER_MEMORY")
    dynamic_allocation_enabled: bool = os.getenv("SPARK_DYNAMICALLOCATION_ENABLED")
    executor_instances_min: str = os.getenv("SPARK_DYNAMICALLOCATION_MINEXECUTORS")
    executor_instances_max: str = os.getenv("SPARK_DYNAMICALLOCATION_MAXEXECUTORS")
    # executor_instances: str = os.getenv("SPARK_EXECUTOR_INSTANCES")
    executor_cores: str = os.getenv("SPARK_EXECUTOR_CORES")
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY")
    driver_cores: str = os.getenv("SPARK_DRIVER_CORES")
    driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY")

### **Websocket Server Configuration**
class WebSocketConfig(BaseModel):
    user: str = os.getenv("WEBSOCKET_USER")
    host: str = os.getenv("WEBSOCKET_HOST")
    port: str = os.getenv("WEBSOCKET_PORT")  # Keep as string

    @property
    def url(self) -> str:
        return f'{self.user}://{self.host}:{self.port}/ws'


### **Airflow Database Configuration**
class AirflowDatabaseConfig(BaseModel):
    version: str = os.getenv("AIRFLOW_VERSION")
    name: str = os.getenv("AIRFLOW_DATABASE_NAME")
    user: str = os.getenv("AIRFLOW_DATABASE_USER")
    password: str = os.getenv("AIRFLOW_DATABASE_PASSWORD")
    host: str = os.getenv("AIRFLOW_DATABASE_HOST")
    port: str = os.getenv("AIRFLOW_DATABASE_PORT")  # Keep as string



# Instantiate global config objects
db_config = DatabaseConfig()
weather_api_config = OpenWeatherConfig()
kafka_config = KafkaConfig()
spark_config = SparkConfig()
websocket_config = WebSocketConfig()
airflow_db_config = AirflowDatabaseConfig()

