from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, lit, to_timestamp, from_unixtime
from sqlalchemy import create_engine, text

from config.properties import db_config as db

# Create Spark session
app_name = 'WeatherStaging'
deps = 'org.postgresql:postgresql:42.6.0'
spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.jars.packages", deps) \
    .getOrCreate()
engine = create_engine(db.url)


def read_table_from_db(table_name):
    return (spark.read
            .format('jdbc')
            .option('url', db.jdbc_url)
            .option('dbtable', table_name)
            .option('user', db.user)
            .option('password', db.password)
            .option('driver', db.driver)
            .load())


def run():
    # read from database (weather_raw)
    df_raw = read_table_from_db(db.table_raw)

    if df_raw.rdd.isEmpty():
        print("No data found in weather_raw.")
        return

    df_staging_old = spark.read \
        .format("jdbc") \
        .option("url", db.jdbc_url) \
        .option("dbtable", db.table_staging) \
        .option("user", db.user) \
        .option("password", db.password) \
        .option("driver", db.driver) \
        .load()

    if df_staging_old.rdd.isEmpty():
        latest_ts = 0
        print("Staging table empty. Processing all records.")
    else:
        latest_ts = df_staging_old.agg({"timestamp": "max"}).collect()[0][0]
        print(f"Latest timestamp in staging: {latest_ts}")

    # Filter for new raw data only
    df_raw_new = df_raw.filter(col("timestamp") > lit(latest_ts))
    if df_raw_new.rdd.isEmpty():
        print("No new data to process.")
        return

    # dedupe the dataframe
    df_deduped = df_raw_new.orderBy(desc('timestamp')).dropDuplicates(['city_id', 'timestamp'])

    # add timestamp column that uses UTC
    df_deduped = df_deduped.withColumn("timestamp_utc", to_timestamp(from_unixtime(col("timestamp"))))

    # extract the city information
    city_cols = ['city_id', 'city', 'country', 'latitude', 'longitude']
    df_city_candidates = df_deduped.select(city_cols).dropDuplicates(['city_id'])
    df_city_existing = read_table_from_db(db.table_cities)

    # Left join on city_id and keep rows that don't match as they need to be inserted
    df_new_cities = df_city_candidates.alias("new").join(df_city_existing.alias("old"), on=col("new.city_id") == col("old.id"), how="left_anti")

    # Insert new cities if the RDD isn't empty
    if df_new_cities.rdd.isEmpty():
        print("No new cities to insert.")
    else:
        insert_cities_sql = f'''
                            INSERT INTO {db.table_cities} (id, name, country, latitude, longitude)
                            VALUES (:id, :name, :country, :latitude, :longitude)
                            ON CONFLICT (id) DO NOTHING
                            '''
        df_new_cities = df_new_cities.toPandas()
        with engine.connect() as conn:
            conn.execute(
                text(insert_cities_sql),
                [
                    {
                        "id": row["city_id"],
                        "name": row["city"],
                        "country": row["country"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"]
                    }
                    for _, row in df_new_cities.iterrows()
                ]
            )

    # drop the city columns but leave city ID as it's the foreign key in the staging table
    city_cols.remove('city_id')
    df_staging = df_deduped.drop(*city_cols)

    # write to db as weather_staging
    df_staging.write \
        .format("jdbc") \
        .option("url", db.jdbc_url) \
        .option("dbtable", db.table_staging) \
        .option("user", db.user) \
        .option("password", db.password) \
        .option("driver", db.driver) \
        .mode("append") \
        .save()

    print(f"Staging data updated. Appended {df_staging.count()} records.")


if __name__ == '__main__':
    run()