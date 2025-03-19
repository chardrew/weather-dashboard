import asyncio
import streamlit as st
import websockets
import pandas as pd
import plotly.express as px
import folium
from streamlit_folium import folium_static
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from datetime import datetime, timezone, timedelta
from config.properties import db_config as db, websocket_config as websocket


def read_table_from_db(table_name):
    spark = SparkSession.builder \
        .appName('Dashboard') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0') \
        .getOrCreate()

    df = spark.read \
        .format('jdbc') \
        .option('url', db.jdbc_url) \
        .option('dbtable', table_name) \
        .option('user', db.user) \
        .option('password', db.password) \
        .option('driver', db.driver) \
        .load()
    return df


def convert_wind_direction(degrees):
    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N']
    return directions[round(degrees / 45)]


def format_unix_time(timestamp, offset):
    return datetime.fromtimestamp(timestamp, timezone.utc) + timedelta(seconds=offset)


def create_map(lat, lon, city):
    m = folium.Map(location=[lat, lon], zoom_start=5)
    folium.Marker([lat, lon], popup=city, tooltip=city).add_to(m)
    return m


def init_dashboard():
    st.session_state["header_placeholder"] = st.empty()
    st.session_state["weather_placeholder"] = st.empty()
    st.session_state["chart_placeholder"] = st.empty()
    st.session_state["map_placeholder"] = st.empty()


def filter_last_24_hrs(dataframe):
    dataframe = dataframe.orderBy(col('timestamp').desc()).dropDuplicates(["timestamp", "city_id"])  # if combination is the same, then drop duplicates
    now_epoch = int(datetime.now(timezone.utc).timestamp())  # Ensure it's an integer
    ago_24hrs_epoch = now_epoch - int(timedelta(hours=24).total_seconds())  # Ensure it's an integer

    return dataframe.filter(dataframe.timestamp >= ago_24hrs_epoch)

def extract_weather_data(row):
    return {
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "temperature": row["temperature"],
        "feels_like": row["feels_like"],
        "humidity": row["humidity"],
        "wind_speed": row["wind_speed"],
        "wind_deg": row["wind_direction"],
        "clouds": row["cloud_coverage"],
        "visibility": row["visibility"],
        "weather": row["weather_description"],
        "weather_icon": row["weather_icon"],
        "sunrise": row["sunrise"],
        "sunset": row["sunset"],
        "timestamp": row["timestamp"],
        "timezone_offset": row["timezone"],
        "city": row["city"]
    }

def update_dashboard():
    # Initialise cache if required
    if "prev_data" not in st.session_state:
        st.session_state["prev_data"] = {}

    # Read from the database
    df_raw = read_table_from_db(db.table_raw)
    # df_agg = read_table_from_db(table_name_agg)

    # Filter to last 24 hours of data
    df_24 = filter_last_24_hrs(df_raw)

    # Get most recent row and bring into memory
    most_recent_row = df_24.orderBy(desc("timestamp")).limit(1).collect()[0]
    new_data = extract_weather_data(most_recent_row)

    # Check if data has changed
    if st.session_state["prev_data"] == new_data:
        return

    # Call update functions
    update_weather_data(new_data)
    update_chart(df_24)
    update_map(new_data)

    # Store for comparison next update_dashboard() call
    st.session_state["prev_data"] = new_data


def update_weather_data(data_row):

    with st.session_state["header_placeholder"]:
        # Convert timestamp to local time and format
        last_updated_local = format_unix_time(data_row["timestamp"], data_row["timezone_offset"])
        last_updated_local_str = last_updated_local.strftime("%A %-d %B, %I:%M %p")

        # Write title
        line1 = '🌤 Real-time Weather Dashboard'
        line2 = f'📍 {data_row["city"]}'
        st.markdown(
            f"""
            <h1 style='font-size: 42px; font-weight: bold; text-align: center; margin-bottom: 0;'>
                {line1} <br>
                {line2}
            </h1>
            <h5 style='text-align: center; color: gray;'>Last updated at: {last_updated_local_str} (Local time)</h5>
            """,
            unsafe_allow_html=True
        )


    with st.session_state["weather_placeholder"].container():
        col1, col2, col3 = st.columns(3)  # first row
        col4, col5, col6 = st.columns(3)  # second row

        # 🌡️ Temperature
        if st.session_state["prev_data"].get("temperature") != data_row["temperature"]:
            col1.metric("🌡️ Temperature (°C)", f"{data_row['temperature']:.1f}°C",
                        f"Feels like {data_row['feels_like']:.1f}°C")

        # 💦 Humidity
        if st.session_state["prev_data"].get("humidity") != data_row["humidity"]:
            col2.metric("💦 Humidity", f"{data_row['humidity']}%")

        # 🌬️ Wind Speed
        if st.session_state["prev_data"].get("wind_speed") != data_row["wind_speed"]:
            wind_direction = convert_wind_direction(data_row["wind_deg"])
            col3.metric("🌬️ Wind", f"{data_row['wind_speed']} m/s {wind_direction}")

        # 🖼️ Weather Icon & Description (Move to Second Row)
        if st.session_state["prev_data"].get("weather") != data_row["weather"]:
            with col4:
                icon_url = f"http://openweathermap.org/img/wn/{data_row['weather_icon']}@2x.png"
                st.image(icon_url, caption=data_row["weather"])

        # ☁️ Cloud Cover
        if st.session_state["prev_data"].get("clouds") != data_row["clouds"]:
            col5.metric("☁️ Cloud Cover", f"{data_row['clouds']}%")

        # 👀 Visibility
        if st.session_state["prev_data"].get("visibility") != data_row["visibility"]:
            col6.metric("👀 Visibility", f"{data_row['visibility'] / 1000:.1f} km")

        # ️🌙 Sunrise and Sunset
        sunrise_time = format_unix_time(data_row["sunrise"], data_row["timezone_offset"])
        sunset_time = format_unix_time(data_row["sunset"], data_row["timezone_offset"])
        daylight_duration = sunset_time - sunrise_time

        if st.session_state["prev_data"].get("sunrise") != data_row["sunrise"]:
            col1, col2 = st.columns(2)

            with col1:  # left side
                st.write(f"☀️ Sunrise: {sunrise_time.strftime('%I:%M %p')} | 🌙 Sunset: {sunset_time.strftime('%I:%M %p')}")

            with col2:  # right side
                st.write(f"⏳ Daylight Duration: {str(daylight_duration).split('.')[0].split(':')[0]} hours, {str(daylight_duration).split('.')[0].split(':')[1]} minutes")


def update_chart(df_spark):
    if not df_spark.take(1):  # More efficient than '.count()'
        st.warning("No temperature data available for the last 24 hours.")
        return # Stop execution if no data is found
    else:
        df_pandas = df_spark.toPandas()  # for plotly

    if "prev_chart_data" in st.session_state:
        prev_data = st.session_state["prev_chart_data"]
        if df_pandas.equals(prev_data):
            return  # Skip update if data hasn't changed

    st.session_state["prev_chart_data"] = df_pandas  # Update with new data
    df_pandas['local_time'] = pd.to_datetime(df_pandas['timestamp'] + df_pandas['timezone'], unit='s')
    fig = px.line(df_pandas, x='local_time', y=['temperature', 'feels_like', 'temp_min', 'temp_max'],
                  title='Temperature Trends',
                  labels={'value': 'Temperature (°C)', 'local_time': 'Local Time'})

    if "chart_placeholder" not in st.session_state:
        st.session_state["chart_placeholder"] = st.empty()
    chart_placeholder = st.session_state["chart_placeholder"]
    chart_placeholder.empty() # clear previous chart to avoid stacking elements
    chart_placeholder.plotly_chart(fig, use_container_width=True)


def update_map(data_row):
    lat, lon, city = data_row["latitude"], data_row["longitude"], data_row["city"]

    if "prev_location" in st.session_state:
        prev_lat, prev_lon = st.session_state["prev_location"]
        if lat == prev_lat and lon == prev_lon:
            return  # Skip update if location hasn't changed

    st.session_state["prev_location"] = (lat, lon)  # Store new location

    with st.session_state["map_placeholder"].container():
        st.write(f"📍 {city}, {lat:.2f}, {lon:.2f}")
        folium_static(create_map(lat, lon, city))

def start_websocket_listener():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(listen_for_updates())


async def listen_for_updates():
    async with websockets.connect(websocket.url) as ws:
        while True:
            msg = await ws.recv()
            if msg == "update":
                update_dashboard()


if __name__ == "__main__":
    init_dashboard()
    asyncio.run(listen_for_updates())