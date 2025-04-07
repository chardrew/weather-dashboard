from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import *
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config.properties import db_config as db, kafka_config as kafka
from plugins.utils import db as db_utils

# Initialize SQLAlchemy engine & session
engine = create_engine(db.url)
Session = sessionmaker(bind=engine)

def write_to_db(batch_df, batch_id):
    print(f"🔍 Processing Batch ID: {batch_id}")
    batch_df.show(truncate=False)  # Print data for debugging

    if batch_df.count() == 0:  # No new records
        print("⚠️ No new records in this batch.")
        return

    print('📥 Saving weather data to Database...')
    batch_df.write \
        .format('jdbc') \
        .option('url', db.jdbc_url) \
        .option('dbtable', db.table_raw) \
        .option('user', db.user) \
        .option('password', db.password) \
        .option('driver', db.driver) \
        .mode('append') \
        .save()

    print('📢 Sending NOTIFY to WebSocket server...')
    session = Session()
    session.execute(text("NOTIFY weather_updates, 'new_data';"))
    session.commit()
    session.close()


def get_schema():
    """Defines the schema for weather data."""
    return StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True)
        ]), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ])), True),
        StructField("base", StringType(), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("sea_level", IntegerType(), True),
            StructField("grnd_level", IntegerType(), True)
        ]), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True),
            StructField("deg", IntegerType(), True),
            StructField("gust", DoubleType(), True)
        ]), True),
        StructField("clouds", StructType([
            StructField("all", IntegerType(), True)
        ]), True),
        StructField("dt", FloatType(), True),
        StructField("sys", StructType([
            StructField("type", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("sunrise", LongType(), True),
            StructField("sunset", LongType(), True)
        ]), True),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("code", IntegerType(), True)
    ])


def flatten(df: DataFrame):
    """Flattens the structured DataFrame."""
    df_flat = df \
        .withColumn("weather_exploded", explode(col("weather"))) \
        .select(
            col("coord.lon").alias("longitude"),
            col("coord.lat").alias("latitude"),
            col("weather_exploded.id").alias("weather_id"),
            col("weather_exploded.main").alias("weather_main"),
            col("weather_exploded.description").alias("weather_description"),
            col("weather_exploded.icon").alias("weather_icon"),
            col("main.temp").alias("temperature"),
            col("main.feels_like").alias("feels_like"),
            col("main.temp_min").alias("temp_min"),
            col("main.temp_max").alias("temp_max"),
            col("main.pressure").alias("pressure"),
            col("main.humidity").alias("humidity"),
            col("main.sea_level").alias("sea_level"),
            col("main.grnd_level").alias("grnd_level"),
            col("visibility"),
            col("wind.speed").alias("wind_speed"),
            col("wind.deg").alias("wind_direction"),
            col("wind.gust").alias("wind_gust"),
            col("clouds.all").alias("cloud_coverage"),
            col("dt").alias("timestamp"),
            col("sys.country").alias("country"),
            col("sys.sunrise").alias("sunrise"),
            col("sys.sunset").alias("sunset"),
            col("timezone"),
            col("id").alias("city_id"),
            col("name").alias("city"),
            col("code").alias("response_code")
        )

    return df_flat.drop('weather_exploded')


def start_streaming():
    """Starts Spark Streaming from Kafka to PostgreSQL."""
    app_name = 'KafkaStreamProcessor'
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    conf = spark.sparkContext.getConf()
    print("Executor cores:", conf.get("spark.executor.cores"))
    print("Driver cores:", conf.get("spark.driver.cores"))
    print("Executor memory:", conf.get("spark.executor.memory"))
    print("Driver memory:", conf.get("spark.driver.memory"))

    schema = get_schema()

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers) \
        .option("subscribe", kafka.topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)")  # Convert bytes to string

    df_selected = df.select(from_json(col("value"), schema).alias("data")).select('data.*')

    # Flatten and remove columns
    columns_to_drop = ['weather_id', 'base', 'sys_type', 'sys_id', 'response_code']
    df_final = flatten(df_selected).drop(*columns_to_drop)

    query = df_final.writeStream \
        .foreachBatch(write_to_db) \
        .outputMode('append') \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    db_utils.init()  # Create database and table if they don't exist
    start_streaming()  # Start streaming data from Kafka to database
