import asyncio
import functools
import json

import streamlit as st
import websockets
import pandas as pd
import folium
from py4j.protocol import Py4JJavaError
from streamlit.errors import StreamlitDuplicateElementId
from streamlit_folium import folium_static
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from datetime import datetime, timezone, timedelta
from config.properties import db_config as db, websocket_config as websocket, weather_api_config as weather_api
import plotly.graph_objs as go

print = functools.partial(print, flush=True)
charts = ['temperature', 'humidity', 'wind', 'cloud']

def read_table_from_db(table_name):
    try:
        app_name = 'Dashboard'
        deps = 'org.postgresql:postgresql:42.6.0'
        spark = SparkSession.builder \
            .appName(app_name) \
            .config('spark.jars.packages', deps) \
            .getOrCreate()

        df = spark.read \
            .format('jdbc') \
            .option('url', db.jdbc_url) \
            .option('dbtable', table_name) \
            .option('user', db.user) \
            .option('password', db.password) \
            .option('driver', db.driver) \
            .load()

        assert not (df is None or df.rdd.isEmpty())
        return df
    except (Py4JJavaError, AssertionError) as ae:
        print(f"[SKIPPED] Table `{table_name}` missing or empty: {ae}")
        return None
    except Exception as e:
        print(f"[ERROR] Could not read `{table_name}`: {e}")
        return None


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
    st.session_state["metrics_placeholder"] = st.empty()
    st.session_state["chart_placeholder_temperature"] = st.empty()
    st.session_state["chart_placeholder_humidity"] = st.empty()
    st.session_state["chart_placeholder_wind"] = st.empty()
    st.session_state["chart_placeholder_cloud"] = st.empty()
    st.session_state["map_placeholder"] = st.empty()


def extract_weather_data(row):
    return {
        "city_id": row["city_id"],
        "city": row["city"],
        "country": row["country"],
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
    }

def update_dashboard():
    # Initialise cache if required
    if "prev_data_stg" not in st.session_state:
        st.session_state["prev_data_stg"] = {}
    if "prev_data_agg" not in st.session_state:
        st.session_state["prev_data_agg"] = {}

    # Read from the database
    df_stg = read_table_from_db(db.table_staging)
    df_agg = read_table_from_db(db.table_agg)
    df_cities = read_table_from_db(db.table_cities)

    if df_stg is None or df_agg is None or df_cities is None:
        return  # table missing - ingestion/staging/aggregation jobs need to finish first

    # Filter to last 24 hours of data
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    df_stg_24 = df_stg.filter(df_stg.timestamp_utc >= cutoff)
    df_agg_24 = df_agg.filter(df_agg.timestamp_hour >= cutoff)

    # Filter by city
    city = df_cities \
                    .filter((col("name") == weather_api.city) & (col("country") == weather_api.country)) \
                    .limit(1) \
                    .collect()[0]

    df_stg_24 = df_stg_24.filter(col('city_id') == city['id'])
    df_agg_24 = df_agg_24.filter(col('city_id') == city['id'])

    # Sort by date for plotting
    df_stg_24 = df_stg_24.sort(desc('timestamp'))
    df_agg_24 = df_agg_24.sort(desc('timestamp_hour'))

    # Bring relevant data into memory
    latest_df = df_stg_24.limit(1)
    joined = latest_df.alias("stg").join(
        df_cities.alias("cities"),
        col("stg.city_id") == col("cities.id"),
        how="left"
    ).withColumnRenamed("name", "city").drop('id')
    latest_row = joined.collect()[0]
    latest_row = extract_weather_data(latest_row)

    df_stg_24 = df_stg_24.toPandas()
    df_agg_24 = df_agg_24.toPandas()

    # Check if data has changed
    is_new_stg = not df_stg_24.equals(st.session_state["prev_data_stg"])
    is_new_agg = not df_agg_24.equals(st.session_state["prev_data_agg"])
    if not is_new_stg and not is_new_agg:
        return

    # Call update functions
    if is_new_stg:
        update_weather_data(latest_row)
        update_map(latest_row)
    if is_new_agg or is_new_stg:
        update_charts(df_stg_24, df_agg_24)

    # Store for comparison next update_dashboard() call
    st.session_state["prev_data_stg"] = df_stg_24
    st.session_state["prev_data_agg"] = df_agg_24


def update_weather_data(new_row):
    # Write header
    with st.session_state["header_placeholder"].container():
        # Convert timestamp to local time and format
        last_updated_local = format_unix_time(new_row["timestamp"], new_row["timezone_offset"])
        last_updated_local_str = last_updated_local.strftime("%A %-d %B, %I:%M %p")

        # Write title
        line1 = '🌤 Real-time Weather Dashboard'
        line2 = f'📍 {new_row["city"]}'
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
    with st.session_state["metrics_placeholder"].container():
        # 2x3 grid
        col1, col2, col3 = st.columns(3)  # first row
        col4, col5, col6 = st.columns(3)  # second row

        # Temperature
        col1.metric("🌡️ Temperature (°C)", f"{new_row['temperature']:.1f}°C",
                    f"Feels like {new_row['feels_like']:.1f}°C")

        # Humidity
        col2.metric("💦 Humidity", f"{new_row['humidity']}%")

        # Wind Speed
        wind_direction = convert_wind_direction(new_row["wind_deg"])
        col3.metric("🌬️ Wind", f"{new_row['wind_speed']} m/s {wind_direction}")

        # Weather Icon & Description
        with col4:
            icon_url = f"http://openweathermap.org/img/wn/{new_row['weather_icon']}@2x.png"
            st.image(icon_url, caption=new_row["weather"])

        # Cloud Cover
        col5.metric("☁️ Cloud Cover", f"{new_row['clouds']}%")

        # Visibility
        col6.metric("👀 Visibility", f"{new_row['visibility'] / 1000:.1f} km")

        # ️ Sunrise and Sunset times
        sunrise_time = format_unix_time(new_row["sunrise"], new_row["timezone_offset"])
        sunset_time = format_unix_time(new_row["sunset"], new_row["timezone_offset"])
        daylight_duration = sunset_time - sunrise_time

        col7, col8 = st.columns(2)
        with col7:  # left side
            st.write(f"☀️ Sunrise: {sunrise_time.strftime('%I:%M %p')} | 🌙 Sunset: {sunset_time.strftime('%I:%M %p')}")
        with col8:  # right side
            hrs, mins, *_ = str(daylight_duration).split(':')
            st.write(f"⏳ Daylight Duration: {hrs} hours, {mins} minutes")


def update_charts(df_stg, df_hourly):
    # add local_time column so plot is in local time
    df_stg['local_time'] = pd.to_datetime(df_stg['timestamp'] + df_stg['timezone'], unit='s')
    df_hourly['local_time'] = df_hourly['timestamp_hour'] + \
                              pd.to_timedelta(30, unit='minutes') + \
                              pd.to_timedelta(df_stg['timezone'], unit='s')

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

    # -- Construct charts --
    # Temperature
    fig_temperature = go.Figure()
    fig_temperature.add_trace(go.Scatter(
        x=df_stg['local_time'],
        y=df_stg['temperature'],
        name='Temperature',
        mode='lines+markers',
        line=dict(color='#1f77b4')
    ))
    fig_temperature.add_trace(go.Scatter(
        x=df_stg['local_time'],
        y=df_stg['feels_like'],
        name='Feels Like',
        line=dict(color='#ff7f0e')
    ))
    fig_temperature.add_trace(go.Scatter(
        x=df_hourly['local_time'],
        y=df_hourly['avg_temp'],
        name='Hourly Avg',
        line=dict(color='#aec7e8', dash='dash', shape='spline', smoothing=1.3),
        opacity=0.7
    ))
    fig_temperature.update_layout(
        title='🌡️ Temperature (°C)',
        xaxis_title='Local Time',
        yaxis_title='°C',
        height=half_height,
        **layout_common
    )

    # Humidity
    fig_humidity = go.Figure()
    fig_humidity.add_trace(go.Scatter(
        x=df_stg['local_time'],
        y=df_stg['humidity'],
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

    # Wind Speed
    fig_wind = go.Figure()
    fig_wind.add_trace(go.Scatter(
        x=df_stg['local_time'],
        y=df_stg['wind_speed'],
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

    # Cloud Coverage
    fig_clouds = go.Figure()
    fig_clouds.add_trace(go.Scatter(
        x=df_stg['local_time'],
        y=df_stg['cloud_coverage'],
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

    # Empty old charts
    for chart in charts:
       st.session_state[f"chart_placeholder_{chart}"].empty()

    # Show charts
    with st.session_state["chart_placeholder_temperature"]:
        st.plotly_chart(fig_temperature, use_container_width=True)
    with st.session_state["chart_placeholder_humidity"]:
        st.plotly_chart(fig_humidity, use_container_width=True)
    with st.session_state["chart_placeholder_wind"]:
        st.plotly_chart(fig_wind, use_container_width=True)
    with st.session_state["chart_placeholder_cloud"]:
        st.plotly_chart(fig_clouds, use_container_width=True)


def update_map(new_loc):
    lat, lon, city = new_loc["latitude"], new_loc["longitude"], new_loc["city"]

    if "prev_location" in st.session_state:
        prev_lat, prev_lon = st.session_state["prev_location"]
        if lat == prev_lat and lon == prev_lon:
            return  # Skip update if location hasn't changed

    m = create_map(lat, lon, city)
    st.write(f"📍 {city},  {lat:.2f}, {lon:.2f}")
    with st.session_state["map_placeholder"]:
        folium_static(m, height=200)

    st.session_state["prev_location"] = (lat, lon)  # Store new location


async def listen_for_updates():
    async with websockets.connect(websocket.url) as ws:
        print(f"WebSocket connected: {websocket.url}")
        while True:
            raw_msg = await ws.recv()
            try:
                msg = json.loads(raw_msg)
                channel = msg.get("channel")
                payload = msg.get("payload")
                print(f"Received message from {channel}: {payload}")

                if channel == "staging_updates" and payload == "new_data":
                    print(f"Triggering dashboard update at {datetime.now()}")
                    update_dashboard()

            except json.JSONDecodeError:
                print(f"Ignoring malformed message: {raw_msg}")


if __name__ == "__main__":
    init_dashboard()
    update_dashboard()
    asyncio.run(listen_for_updates())