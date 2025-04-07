from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, date_trunc, from_unixtime
from config.properties import db_config as db

# Create a new Spark session for transformations
app_name = 'WeatherTransform'
deps = 'org.postgresql:postgresql:42.6.0'
spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.jars.packages', deps) \
        .getOrCreate()


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
        .withColumn("timestamp_ts", from_unixtime(col("timestamp"))) \
        .withColumn("timestamp_hour", date_trunc("hour", col("timestamp_ts"))) \
        .groupBy("city_id", "city", "timestamp_hour") \
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("humidity").alias("avg_humidity"),
            avg("wind_speed").alias("avg_wind_speed"),
            avg("cloud_coverage").alias("avg_cloud_coverage")
        )
    df_hourly.drop("timestamp_ts")

    # Save aggregated results to PostgreSQL
    df_hourly.write \
     .format('jdbc') \
     .option('url', db.jdbc_url) \
     .option('dbtable', db.table_agg) \
     .option('user', db.user) \
     .option('password', db.password) \
     .option('driver', db.driver) \
     .mode('overwrite') \
     .save()

    print("Aggregated weather data successfully saved to PostgreSQL.")
