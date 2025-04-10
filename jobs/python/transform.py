from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, date_trunc, from_unixtime, to_utc_timestamp, to_timestamp
from sqlalchemy import create_engine

from config.properties import db_config as db

# Create a new Spark session for transformations
app_name = 'WeatherTransform'
deps = 'org.postgresql:postgresql:42.6.0'
spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.jars.packages', deps) \
        .getOrCreate()

engine = create_engine(db.url)


def read_table_from_db(table_name):
    """Reads a table from PostgreSQL into a Spark DataFrame."""
    return (spark.read
            .format('jdbc')
            .option('url', db.jdbc_url)
            .option('dbtable', table_name)
            .option('user', db.user)
            .option('password', db.password)
            .option('driver', db.driver)
            .load())


if __name__ == '__main__':
    df_raw = read_table_from_db(db.table_raw)

    # Perform hourly aggregations
    df_hourly = df_raw \
        .withColumn("timestamp_utc", to_utc_timestamp(from_unixtime(col("timestamp")), "UTC")) \
        .withColumn("timestamp_hour", date_trunc("hour", col("timestamp_utc"))) \
        .groupBy("city_id", "timestamp_hour") \
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("humidity").alias("avg_humidity"),
            avg("wind_speed").alias("avg_wind_speed"),
            avg("cloud_coverage").alias("avg_cloud_coverage")
        )

    # Save aggregated results to PostgreSQL
    engine.execute(f'TRUNCATE TABLE {db.table_agg};')  # empty table but keep schema in tact
    df_hourly.write \
        .format("jdbc") \
        .option("url", db.jdbc_url) \
        .option("dbtable", db.table_agg) \
        .option("user", db.user) \
        .option("password", db.password) \
        .option("driver", db.driver) \
        .mode("append") \
        .save()

    print("Aggregated weather data successfully saved to PostgreSQL.")
