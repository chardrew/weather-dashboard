import psycopg2
from config.properties import db_config as db


def init():
    print(f'Initializing database: {db.name}, '
          f'Tables: {db.table_raw}, {db.table_staging}, {db.table_cities}, {db.table_agg}, '
          f'User: {db.user}'
    )

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

    # raw data table  # todo add foreign key for cities table in raw table ->  cities(id)
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

    # staging table
    create_table_staging = f'''
    CREATE TABLE IF NOT EXISTS {db.table_staging} (
        id INTEGER PRIMARY KEY,
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
        timestamp_utc TIMESTAMPTZ,
        sunrise BIGINT,
        sunset BIGINT,
        timezone INTEGER,
        city_id INTEGER,
        FOREIGN KEY (city_id) REFERENCES {db.table_cities}(id)
    );
    '''

    # cities table
    create_table_cities = f'''
        CREATE TABLE IF NOT EXISTS {db.table_cities} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country VARCHAR(5),
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL
        );
    '''

    # hourly aggregate table
    create_table_hourly_agg = f'''
        CREATE TABLE IF NOT EXISTS {db.table_agg} (
            city_id INTEGER,
            timestamp_hour TIMESTAMPTZ,
            avg_temp FLOAT,
            avg_humidity FLOAT,
            avg_wind_speed FLOAT,
            avg_cloud_coverage FLOAT,
            PRIMARY KEY (city_id, timestamp_hour),
            FOREIGN KEY (city_id) REFERENCES {db.table_cities}(id)
        );
    '''

    # NOTIFY on new insert into raw table
    raw_update_func = '''
    CREATE OR REPLACE FUNCTION notify_raw_update()
    RETURNS TRIGGER AS $$
    DECLARE
        last_row weather_raw;
    BEGIN
        -- Get the most recent row (excluding the new one being inserted)
        SELECT * INTO last_row
        FROM weather_raw
        WHERE city_id = NEW.city_id
        ORDER BY timestamp DESC
        LIMIT 1;

        -- Compare selected fields
        IF last_row IS NULL OR
           last_row.timestamp IS DISTINCT FROM NEW.timestamp THEN
            PERFORM pg_notify('raw_updates', 'new_data');
        END IF;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    '''

    raw_update_trig = f'''
           DO $$ 
           BEGIN
               IF NOT EXISTS (
                   SELECT 1 FROM pg_trigger WHERE tgname = 'raw_update_trigger'
               ) THEN
                   CREATE TRIGGER raw_update_trigger
                   AFTER INSERT ON {db.table_raw}
                   FOR EACH STATEMENT
                   EXECUTE FUNCTION notify_raw_update();
               END IF;
           END $$;
       '''

    # NOTIFY on new insert into staging table
    stg_update_func = f'''
        CREATE OR REPLACE FUNCTION notify_staging_update()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('staging_updates', 'new_data');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    '''

    stg_update_trig = f'''
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'staging_update_trigger'
            ) THEN
                CREATE TRIGGER staging_update_trigger
                AFTER INSERT ON {db.table_staging}
                FOR EACH STATEMENT
                EXECUTE FUNCTION notify_staging_update();
            END IF;
        END $$;
    '''

    # NOTIFY on new insert into hourly table
    hourly_update_func = f'''
            CREATE OR REPLACE FUNCTION notify_hourly_update()
            RETURNS TRIGGER AS $$
            BEGIN
                PERFORM pg_notify('hourly_updates', 'new_data');
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        '''

    hourly_update_trig = f'''
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_trigger WHERE tgname = 'hourly_update_trigger'
                ) THEN
                    CREATE TRIGGER hourly_update_trigger
                    AFTER INSERT ON {db.table_agg}
                    FOR EACH STATEMENT
                    EXECUTE FUNCTION notify_hourly_update();
                END IF;
            END $$;
        '''
    # Execute the above statements
    cur = conn.cursor()
    cur.execute(create_table_raw)
    cur.execute(create_table_cities)
    cur.execute(create_table_staging)
    cur.execute(create_table_hourly_agg)
    cur.execute(raw_update_func)
    cur.execute(raw_update_trig)
    cur.execute(stg_update_func)
    cur.execute(stg_update_trig)
    cur.execute(hourly_update_func)
    cur.execute(hourly_update_trig)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Database {db.name} initialized with NOTIFY triggers "
          f"on {db.table_raw}, {db.table_staging} and {db.table_agg} tables.")


if __name__ == "__main__":
    init()
