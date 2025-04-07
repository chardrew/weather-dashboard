import asyncio
import streamlit as st
import websockets
import pandas as pd
import folium
from streamlit_folium import folium_static, st_folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, to_unix_timestamp, to_timestamp, from_unixtime, lit
from datetime import datetime, timezone, timedelta
from config.properties import db_config as db, websocket_config as websocket
import plotly.graph_objs as go


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
    st.session_state["chart_placeholder_temperature"] = st.empty()
    st.session_state["chart_placeholder_humidity"] = st.empty()
    st.session_state["chart_placeholder_wind"] = st.empty()
    st.session_state["chart_placeholder_cloud"] = st.empty()
    st.session_state["map_placeholder"] = st.empty()

    with st.session_state["header_placeholder"]:
        # Write title
        line1 = '🌤 Real-time Weather Dashboard'
        st.markdown(
            f"""
            <h1 style='font-size: 42px; font-weight: bold; text-align: center; margin-bottom: 0;'>
                {line1}
            </h1>
            """,
            unsafe_allow_html=True
        )

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
    if "prev_data_raw" not in st.session_state:
        st.session_state["prev_data_raw"] = {}
    if "prev_data_agg" not in st.session_state:
        st.session_state["prev_data_agg"] = {}

    # Read from the database
    try:
        df_raw = read_table_from_db(db.table_raw)
        df_agg = read_table_from_db(db.table_agg)
    except Exception as e:
        print(f'Error reading from database. Message: {e}')
        return

    # Drop duplicates
    df_raw = df_raw.orderBy(desc("timestamp")).dropDuplicates(["timestamp", "city_id"])

    # Filter to last 24 hours of data
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    df_raw_24 = df_raw.filter(to_timestamp(from_unixtime(col("timestamp"))) >= lit(cutoff))
    df_agg_24 = df_agg.filter(df_agg.timestamp_hour >= cutoff)

    # Get most recent row and bring into memory
    most_recent_row = df_raw_24.orderBy(desc("timestamp")).limit(1).collect()[0]
    new_data_raw = extract_weather_data(most_recent_row)

    # Check if data has changed
    is_new_raw = st.session_state["prev_data_raw"] != new_data_raw
    is_new_agg = st.session_state["prev_data_agg"] != df_agg_24
    if not is_new_raw and not is_new_agg:
        return

    # Call update functions
    if is_new_raw:
        update_weather_data(new_data_raw)
        update_map(new_data_raw)
    if is_new_agg or is_new_raw:
        update_charts(df_raw_24, df_agg_24)

    # Store for comparison next update_dashboard() call
    st.session_state["prev_data_raw"] = new_data_raw
    st.session_state["prev_data_agg"] = df_agg_24


def update_weather_data(data_row):

    # Write header
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

    # Weather metrics
    with st.session_state["weather_placeholder"].container():
        # 2x3 grid
        col1, col2, col3 = st.columns(3)  # first row
        col4, col5, col6 = st.columns(3)  # second row

        # Temperature
        if st.session_state["prev_data_raw"].get("temperature") != data_row["temperature"]:
            col1.metric("🌡️ Temperature (°C)", f"{data_row['temperature']:.1f}°C",
                        f"Feels like {data_row['feels_like']:.1f}°C")

        # Humidity
        if st.session_state["prev_data_raw"].get("humidity") != data_row["humidity"]:
            col2.metric("💦 Humidity", f"{data_row['humidity']}%")

        # Wind Speed
        if st.session_state["prev_data_raw"].get("wind_speed") != data_row["wind_speed"]:
            wind_direction = convert_wind_direction(data_row["wind_deg"])
            col3.metric("🌬️ Wind", f"{data_row['wind_speed']} m/s {wind_direction}")

        # Weather Icon & Description
        if st.session_state["prev_data_raw"].get("weather") != data_row["weather"]:
            with col4:
                icon_url = f"http://openweathermap.org/img/wn/{data_row['weather_icon']}@2x.png"
                st.image(icon_url, caption=data_row["weather"])

        # Cloud Cover
        if st.session_state["prev_data_raw"].get("clouds") != data_row["clouds"]:
            col5.metric("☁️ Cloud Cover", f"{data_row['clouds']}%")

        # Visibility
        if st.session_state["prev_data_raw"].get("visibility") != data_row["visibility"]:
            col6.metric("👀 Visibility", f"{data_row['visibility'] / 1000:.1f} km")

        # ️ Sunrise and Sunset times
        sunrise_time = format_unix_time(data_row["sunrise"], data_row["timezone_offset"])
        sunset_time = format_unix_time(data_row["sunset"], data_row["timezone_offset"])
        daylight_duration = sunset_time - sunrise_time

        if st.session_state["prev_data_raw"].get("sunrise") != data_row["sunrise"]:
            col1, col2 = st.columns(2)

            with col1:  # left side
                st.write(f"☀️ Sunrise: {sunrise_time.strftime('%I:%M %p')} | 🌙 Sunset: {sunset_time.strftime('%I:%M %p')}")

            with col2:  # right side
                st.write(f"⏳ Daylight Duration: {str(daylight_duration).split('.')[0].split(':')[0]} hours, {str(daylight_duration).split('.')[0].split(':')[1]} minutes")


def update_charts(df_raw_spark, df_agg_spark):
   # Check if dataframes are empty, else bring into memory for plotting
    if not df_raw_spark.take(1):  # More efficient than '.count()'
        st.warning("No temperature data available for the last 24 hours.")
        return # Stop execution if no data is found
    else:
        df_raw = df_raw_spark.orderBy('timestamp', desc=False).toPandas()

    if not df_agg_spark.take(1):
        st.warning("No aggregate data available for last 24 hours.")
        df_hourly = None
    else:
        df_hourly = df_agg_spark.orderBy('timestamp_hour', desc=False).toPandas()  # for plotly

    # add local_time column so plot is in local time
    df_raw['local_time'] = pd.to_datetime(df_raw['timestamp'] + df_raw['timezone'], unit='s')
    df_hourly['local_time'] = df_hourly['timestamp_hour'] + \
                              pd.to_timedelta(30, unit='m') + \
                              pd.to_timedelta(df_raw['timezone'], unit='s')  # todo change timezone raw after datamodel changes

    # initialise chart placeholders
    if "chart_placeholder_temperature" not in st.session_state:
        st.session_state["chart_placeholder_temperature"] = st.empty()
    if "chart_placeholder_humidity" not in st.session_state:
        st.session_state["chart_placeholder_humidity"] = st.empty()
    if "chart_placeholder_wind" not in st.session_state:
        st.session_state["chart_placeholder_wind"] = st.empty()
    if "chart_placeholder_cloud" not in st.session_state:
        st.session_state["chart_placeholder_cloud"] = st.empty()

    # clear previous chart to avoid stacking elements
    chart_placeholder_temperature = st.session_state["chart_placeholder_temperature"]
    chart_placeholder_humidity = st.session_state["chart_placeholder_humidity"]
    chart_placeholder_wind = st.session_state["chart_placeholder_wind"]
    chart_placeholder_cloud = st.session_state["chart_placeholder_cloud"]
    chart_placeholder_temperature.empty()
    chart_placeholder_humidity.empty()
    chart_placeholder_wind.empty()
    chart_placeholder_cloud.empty()

    # Shared layout styling
    full_height = 500
    half_height = round(0.5*full_height)
    layout_common = dict(
        template='plotly_white',
        font=dict(family="Arial", size=14),
        margin=dict(l=40, r=30, t=40, b=40),
        hovermode='x unified',
        xaxis=dict(showgrid=True, gridcolor='lightgrey'),
        yaxis=dict(showgrid=True, gridcolor='lightgrey')
    )

    # Temperature Chart
    fig_temp = go.Figure()
    fig_temp.add_trace(go.Scatter(
        x=df_raw['local_time'],
        y=df_raw['temperature'],
        name='Temperature',
        mode='lines+markers',
        line=dict(color='#1f77b4')
    ))
    fig_temp.add_trace(go.Scatter(
        x=df_raw['local_time'],
        y=df_raw['feels_like'],
        name='Feels Like',
        line=dict(color='#ff7f0e')
    ))
    fig_temp.add_trace(go.Scatter(
        x=df_hourly['local_time'],
        y=df_hourly['avg_temp'],
        name='Hourly Avg',
        line=dict(color='#aec7e8', dash='dash', shape='spline', smoothing=1.3),
        opacity=0.7
    ))
    fig_temp.update_layout(
        title='🌡️ Temperature (°C)',
        xaxis_title='Local Time',
        yaxis_title='°C',
        height=half_height,
        **layout_common
    )

    # Humidity Chart
    fig_humidity = go.Figure()
    fig_humidity.add_trace(go.Scatter(
        x=df_raw['local_time'],
        y=df_raw['humidity'],
        name='Humidity',
        mode='lines+markers',
        line=dict(color='#2ca02c')
    ))
    if 'avg_humidity' in df_hourly.columns:
        fig_humidity.add_trace(go.Scatter(
            x=df_hourly['local_time'],
            y=df_hourly['avg_humidity'],
            name='Hourly Avg',
            line=dict(color='#98df8a', dash='dash', shape='spline', smoothing=1.3),
            opacity=0.7
        ))
    fig_humidity.update_layout(
        title='💦 Humidity (%)',
        xaxis_title='Local Time',
        yaxis_title='%',
        height=half_height,
        **layout_common
    )

    # Wind Speed Chart
    fig_wind = go.Figure()
    fig_wind.add_trace(go.Scatter(
        x=df_raw['local_time'],
        y=df_raw['wind_speed'],
        name='Wind Speed',
        mode='lines+markers',
        line=dict(color='#9467bd')
    ))
    if 'avg_wind_speed' in df_hourly.columns:
        fig_wind.add_trace(go.Scatter(
            x=df_hourly['local_time'],
            y=df_hourly['avg_wind_speed'],
            name='Hourly Avg',
            line=dict(color='#c5b0d5', dash='dash', shape='spline', smoothing=1.3),
            opacity=0.7
        ))
    fig_wind.update_layout(
        title='🌬️ Wind Speed (m/s)',
        xaxis_title='Local Time',
        yaxis_title='m/s',
        height=half_height,
        **layout_common
    )

    # Cloud Coverage Chart
    fig_clouds = go.Figure()
    fig_clouds.add_trace(go.Scatter(
        x=df_raw['local_time'],
        y=df_raw['cloud_coverage'],
        name='Cloud Coverage (%)',
        mode='lines+markers',
        line=dict(color='#4d4d4d', dash='dot')
    ))

    if 'avg_cloud_coverage' in df_hourly.columns:
        fig_clouds.add_trace(go.Scatter(
            x=df_hourly['local_time'],
            y=df_hourly['avg_cloud_coverage'],
            name='Hourly Avg',
            line=dict(color='#a0a0a0', dash='dash', shape='spline', smoothing=1.3),
            opacity=0.7
        ))

    fig_clouds.update_layout(
        title='☁️ Cloud Coverage (%)',
        xaxis_title='Local Time',
        yaxis_title='%',
        height=half_height,
        **layout_common
    )

    # Show charts
    chart_placeholder_temperature.plotly_chart(fig_temp, use_container_width=True)
    chart_placeholder_humidity.plotly_chart(fig_humidity, use_container_width=True)
    chart_placeholder_wind.plotly_chart(fig_wind, use_container_width=True)
    chart_placeholder_cloud.plotly_chart(fig_clouds, use_container_width=True)


def update_map(data_row):
    lat, lon, city = data_row["latitude"], data_row["longitude"], data_row["city"]

    if "prev_location" in st.session_state:
        prev_lat, prev_lon = st.session_state["prev_location"]
        if lat == prev_lat and lon == prev_lon:
            return  # Skip update if location hasn't changed

    st.session_state["prev_location"] = (lat, lon)  # Store new location

    with st.session_state["map_placeholder"].container():
        st.write(f"📍 {city},  {lat:.2f}, {lon:.2f}")
        folium_static(create_map(lat, lon, city), height=200)

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
    update_dashboard()
    asyncio.run(listen_for_updates())