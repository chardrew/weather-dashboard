from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, max, min, avg
from config.properties import db_config as db

# Create a new Spark session for transformations
spark = (SparkSession.builder
        .appName('Weather_Transform')
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0')
        .getOrCreate())


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
    df = read_table_from_db(db.table_raw)

    # Perform daily aggregations
    df_agg = (df.withColumn('day', to_date(col('timestamp')))
              .groupBy('city', 'day')
              .agg(
                  min(col('temperature')).alias('min_temperature'),
                  max(col('temperature')).alias('max_temperature'),
                  avg(col('temperature')).alias('avg_temperature'),
                  min(col('humidity')).alias('min_humidity'),
                  max(col('humidity')).alias('max_humidity'),
                  avg(col('humidity')).alias('avg_humidity')
              ))

    # Save aggregated results to PostgreSQL
    (df_agg.write
     .format('jdbc')
     .option('url', db.jdbc_url)
     .option('dbtable', db.table_agg)
     .option('user', db.user)
     .option('password', db.password)
     .option('driver', db.driver)
     .mode('overwrite')
     .save())

    print("✅ Aggregated weather data successfully saved to PostgreSQL.")
