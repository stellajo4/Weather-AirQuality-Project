import sqlite3
from datetime import datetime
import requests

# Fetch AQI data using the aqicn API
def get_aqi_data(city):
    """Get AQI data for a city."""
    api_url = f'https://api.waqi.info/feed/{city}/?token=2f4c73a57b156c067a7fb45fa17784bd26cd0a8f'
    return get_api_data(api_url)

# Fetch weather data using the openweathermap API
def get_weather_data_for_city(city):
    """Get weather data for a city."""
    from accuWeather import get_city_weather 
    return get_city_weather(city)

# General API data fetch function
def get_api_data(api_url):
    """
    Fetches data from the API using the provided URL.
    """
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Create the combined AQI and weather data table
def create_combined_table(db_cursor):
    """Create the combined AQI and weather data table."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_combined_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        aqi INTEGER,
        timestamp TEXT,
        dominant_pollutant INTEGER,
        forecast_pm25_avg REAL,
        forecast_pm10_avg REAL,
        forecast_o3_avg REAL,
        temperature REAL,
        weather_description TEXT,
        humidity INTEGER,
        wind_speed REAL,
        UNIQUE(city, timestamp)  -- Ensuring no duplicates
    )
    ''')
    db_cursor.connection.commit()

# Insert combined AQI and weather data into the database
def insert_combined_data(db_cursor, combined_data):
    """Insert combined AQI and weather data into the database."""
    for data in combined_data:
        city_data = data['city_data']
        weather_data = data['weather_data']

        timestamp = city_data['timestamp']  # Using timestamp from AQI data
        db_cursor.execute('''
        INSERT OR REPLACE INTO city_combined_data (city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, 
            forecast_pm10_avg, forecast_o3_avg, temperature, weather_description, humidity, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            city_data['city'], city_data['aqi'], timestamp, city_data['dominant_pollutant'],
            city_data['forecast_pm25_avg'], city_data['forecast_pm10_avg'], city_data['forecast_o3_avg'],
            weather_data['temperature'], weather_data['weather_description'],
            weather_data['humidity'], weather_data['wind_speed']
        ))
    db_cursor.connection.commit()

# Fetch both AQI and weather data for a single city
def get_combined_city_data(city):
    """Fetch both AQI and weather data for a city."""
    aqi_data = get_aqi_data(city)
    
    if aqi_data and aqi_data.get('status') == 'ok':
        city_data = {
            'city': city,
            'aqi': aqi_data['data']['aqi'],
            'timestamp': aqi_data['data']['time']['s'],
            'dominant_pollutant': aqi_data['data'].get('dominentpol', 'N/A'),
            'forecast_pm25_avg': aqi_data['data']['forecast']['daily']['pm25'][0].get('avg', 0),
            'forecast_pm10_avg': aqi_data['data']['forecast']['daily']['pm10'][0].get('avg', 0),
            'forecast_o3_avg': aqi_data['data']['forecast']['daily']['o3'][0].get('avg', 0)
        }
    else:
        print(f"Failed to retrieve AQI data for {city}.")
        return None

    # Fetch weather data
    weather_data = get_weather_data_for_city(city)
    
    if weather_data and weather_data.get('cod') == 200:
        weather_info = {
            'temperature': weather_data['main']['temp'] - 273.15,  # Convert from Kelvin to Celsius
            'weather_description': weather_data['weather'][0]['description'],
            'humidity': weather_data['main']['humidity'],
            'wind_speed': weather_data['wind']['speed']
        }
    else:
        print(f"Failed to retrieve weather data for {city}.")
        return None
    
    return {'city_data': city_data, 'weather_data': weather_info}

# Retrieve and insert combined AQI and weather data for multiple cities
def get_multiple_city_combined_data(city_requests, db_cursor):
    """Retrieve and insert combined AQI and weather data for multiple cities."""
    combined_data = []

    for city in city_requests:
        print(f"Fetching combined data for {city}...")
        data = get_combined_city_data(city)
        
        if data:
            combined_data.append(data)
        else:
            print(f"Skipping {city} due to data retrieval failure.")

    if combined_data:
        insert_combined_data(db_cursor, combined_data)
