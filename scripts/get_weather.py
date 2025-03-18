import time
import requests
import json
from kafka import KafkaProducer
from config.properties import kafka_config as kafka, weather_api_config as weather_api


# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka.bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)


def get_current_weather(city: str):
    """Fetches weather data from OpenWeather API and sends it to Kafka."""
    assert weather_api.api_key is not None, "❌ OpenWeather API key not found"

    query_params = f'?q={city}&appid={weather_api.api_key}&units=metric'
    url = weather_api.api_url + query_params

    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ Error fetching weather data. Response code: {response.status_code}")
        return

    weather_data = response.json()

    # Extract relevant fields
    weather_info = {
        'city': weather_data['name'],
        'temperature': float(weather_data['main']['temp']),  # Convert to float
        'humidity': float(weather_data['main']['humidity']),  # Convert to float
        'weather': weather_data['weather'][0]['description'],
        'timestamp': int(time.time())  # Integer timestamp
    }

    # Send to Kafka topic
    print(f"📡 Sending weather data to Kafka for {city}...\n{json.dumps(weather_info, indent=4)}")

    try:
        producer.send(kafka.topic, weather_info)
        producer.flush()  # Ensure messages are delivered
        print(f"✅ SUCCESS: Sent to topic {kafka.topic}")
    except Exception as e:
        print(f"❌ ERROR sending message to Kafka: {e}")


if __name__ == '__main__':
    interval = 60  # Seconds
    city_name = "Melbourne,AU"
    assert city_name not in ["", None]

    while True:
        get_current_weather(city_name)
        time.sleep(interval)
