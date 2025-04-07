import psycopg2
from PIL.ExifTags import IFD

from config.properties import db_config as db


def init():
    print(f'Initializing database: {db.name}, Table: {db.table_raw}, User: {db.user}')

    # Connect to PostgreSQL and create the database if it doesn't exist
    conn = psycopg2.connect(user=db.user, password=db.password, host=db.host, port=db.port)
    conn.autocommit = True  # Required for CREATE DATABASE
    cur = conn.cursor()

    try:
        cur.execute(f'CREATE DATABASE {db.name};')
    except psycopg2.errors.DuplicateDatabase:
        pass  # Database already exists
    finally:
        conn.autocommit = False
        cur.close()
        conn.close()

    # Connect to the newly created database
    conn = psycopg2.connect(database=db.name, user=db.user, password=db.password, host=db.host, port=db.port)

    # raw data table
    create_table_raw = f'''
    CREATE TABLE IF NOT EXISTS {db.table_raw} (
        id SERIAL PRIMARY KEY,
        longitude FLOAT,
        latitude FLOAT,
        weather_main TEXT,
        weather_description TEXT,
        weather_icon TEXT,
        temperature FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure INTEGER,
        humidity INTEGER,
        sea_level INTEGER,
        grnd_level INTEGER,
        visibility INTEGER,
        wind_speed FLOAT,
        wind_direction INTEGER,
        wind_gust FLOAT,
        cloud_coverage INTEGER,
        timestamp BIGINT,
        country VARCHAR(5),
        sunrise BIGINT,
        sunset BIGINT,
        timezone INTEGER,
        city_id INTEGER,
        city TEXT
    );
    '''

    # hourly aggregate table
    create_table_hourly_agg = f'''
    CREATE TABLE IF NOT EXISTS {db.table_agg} (
        city_id INTEGER,
        city TEXT,
        timestamp_hour TIMESTAMP,
        avg_temp FLOAT,
        avg_humidity FLOAT,
        avg_wind_speed FLOAT,
        avg_cloud_coverage FLOAT,
        PRIMARY KEY(city_id, timestamp_hour)
    );
    '''

    cur = conn.cursor()
    cur.execute(create_table_raw)
    cur.execute(create_table_hourly_agg)

    # Create function for NOTIFY event
    cur.execute("""
        CREATE OR REPLACE FUNCTION notify_weather_update()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('weather_updates', 'update');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Create trigger for raw table if it doesn't exist
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'weather_update_trigger'
            ) THEN
                CREATE TRIGGER weather_update_trigger
                AFTER INSERT ON weather_raw
                FOR EACH ROW 
                EXECUTE FUNCTION notify_weather_update();
            END IF;
        END $$;
    """)

    print("Database initialized with 'weather_update' NOTIFY trigger.")
    conn.commit()
    cur.close()
    conn.close()
#todo fix hardcoded names in trigger and notify

if __name__ == "__main__":
    init()
