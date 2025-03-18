import time
import json
import numpy as np
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from config.properties import kafka_config as kafka


# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka.bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# Define base trends for sinusoidal variation
start_time = datetime.now(timezone.utc)
sunrise_time = start_time.replace(hour=6, minute=0, second=0)
sunset_time = start_time.replace(hour=18, minute=0, second=0)

# Define sinusoidal base values for temperature, humidity, etc.
TEMP_MIN, TEMP_MAX = -5, 30  # Celsius
HUMIDITY_MIN, HUMIDITY_MAX = 30, 90
PRESSURE_MEAN = 1013
WIND_SPEED_MIN, WIND_SPEED_MAX = 2, 15
WIND_DIRECTION_MEAN = 180  # Degrees


def generate_smooth_value(base, variation, period, time_elapsed):
    """Generate smooth sinusoidal variations over time."""
    return base + variation * np.sin(2 * np.pi * time_elapsed / period)


def get_current_weather(city: str, country: str, time_elapsed: int):
    """Generate realistic weather data with smooth trends."""

    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp())

    # Generate smooth variations for weather parameters
    temp = generate_smooth_value((TEMP_MIN + TEMP_MAX) / 2, (TEMP_MAX - TEMP_MIN) / 2, 24 * 60, time_elapsed)
    feels_like = temp + random.uniform(-2, 2)
    temp_min = temp - random.uniform(0, 3)
    temp_max = temp + random.uniform(0, 3)
    humidity = generate_smooth_value((HUMIDITY_MIN + HUMIDITY_MAX) / 2, (HUMIDITY_MAX - HUMIDITY_MIN) / 2, 24 * 60,
                                     time_elapsed)
    pressure = generate_smooth_value(PRESSURE_MEAN, 3, 12 * 60, time_elapsed)
    wind_speed = generate_smooth_value((WIND_SPEED_MIN + WIND_SPEED_MAX) / 2, (WIND_SPEED_MAX - WIND_SPEED_MIN) / 8,
                                       8 * 60, time_elapsed)
    wind_direction = int(generate_smooth_value(WIND_DIRECTION_MEAN, 60, 6 * 60, time_elapsed))
    wind_gust = wind_speed + random.uniform(0, 5)
    cloud_cover = int(abs(np.sin(2 * np.pi * time_elapsed / (12 * 60)) * 100))
    visibility = int(generate_smooth_value(8000, 2000, 24 * 60, time_elapsed))
    weather_desc = "clear sky" if cloud_cover < 20 else "cloudy"
    weather_main = "Clear" if cloud_cover < 20 else "Clouds"
    icon = "01d" if cloud_cover < 20 else "03d"

    # Create weather data JSON
    weather_data = {
        'coord': {'lat': 51.5085, 'lon': -0.1257},  # London
        'weather': [{
            'id': 800,
            'main': weather_main,
            'description': weather_desc,
            'icon': icon
        }],
        'base': 'stations',
        'main': {
            'temp': round(temp, 2),
            'feels_like': round(feels_like, 2),
            'temp_min': round(temp_min, 2),
            'temp_max': round(temp_max, 2),
            'pressure': int(pressure),
            'humidity': int(humidity),
            'sea_level': int(pressure + random.uniform(0, 5)),
            'grnd_level': int(pressure + random.uniform(0, 10))
        },
        'visibility': visibility,
        'wind': {
            'speed': round(wind_speed, 2),
            'deg': wind_direction,
            'gust': round(wind_gust, 2)
        },
        'clouds': {'all': cloud_cover},
        'dt': timestamp,
        'sys': {
            'type': 1,
            'id': 4610,
            'country': country,
            'sunrise': int(sunrise_time.timestamp()),
            'sunset': int(sunset_time.timestamp())
        },
        'timezone': 0,
        'id': 5128581,
        'name': city,
        'code': 200
    }

    # Send to Kafka topic
    print(f"📡 Sending weather data to Kafka for {city}...\n \
                {json.dumps(weather_data, indent=4)}")

    try:
        producer.send(kafka.topic, weather_data)
        producer.flush()  # Ensure messages are delivered
        print(f"✅ SUCCESS: Sent to topic {kafka.topic}")
    except Exception as e:
        print(f"❌ ERROR sending message to Kafka: {e}")


if __name__ == '__main__':
    interval = 60  # Seconds
    time_elapsed = 0  # Track smooth variation over time

    while True:
        city, country = 'London', 'UK'
        get_current_weather(city, country, time_elapsed)
        time_elapsed += 1
        time.sleep(interval)