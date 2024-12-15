import sqlite3
from datetime import datetime
from file_functions import get_api_data

def get_city_weather(city):
    """Get weather data for a city."""
    from file_functions import get_api_data
    api_token = "3c3f7d2d9f242452b9c1389c3172c412"
    request_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_token}"
    return get_api_data(request_url)

def create_weather_table(db_cursor):
    """Create the weather data table in the database."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        temperature REAL,
        weather_description TEXT,
        timestamp TEXT,
        humidity INTEGER,
        wind_speed REAL,
        UNIQUE(city, timestamp)
    )
    ''')

def insert_weather_data(db_cursor, city_data_list):
    """Insert weather data into the database."""
    for city_data in city_data_list:
        city, temperature, description, timestamp, humidity, wind_speed = city_data
        
        db_cursor.execute('''
        SELECT 1 FROM city_weather WHERE city = ? AND timestamp = ?
        ''', (city, timestamp))
        
        if db_cursor.fetchone():
            print(f"Skipping {city} for timestamp {timestamp}: Data already exists.")
        else:
            db_cursor.execute('''
            INSERT OR IGNORE INTO city_weather (city, temperature, weather_description, timestamp, humidity, wind_speed)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (city, temperature, description, timestamp, humidity, wind_speed))
            print(f"Inserted {city} weather data for timestamp {timestamp}.")
    
    db_cursor.connection.commit()

def get_multiple_city_weather(city_requests, db_cursor):
    """Retrieve and insert weather data for multiple cities."""
    all_city_data = []

    for city in city_requests:
        print(f"Fetching weather data for {city}...")
        weather_data = get_city_weather(city)
        
        if weather_data and weather_data.get('cod') == 200:
            city_name = city
            temperature = weather_data['main']['temp'] - 273.15  # Convert from Kelvin to Celsius
            description = weather_data['weather'][0]['description']
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            humidity = weather_data['main']['humidity']
            wind_speed = weather_data['wind']['speed']

            city_data_tuple = (city_name, temperature, description, timestamp, humidity, wind_speed)
            all_city_data.append(city_data_tuple)
        else:
            print(f"Failed to retrieve data for {city}.")

    if all_city_data:
        insert_weather_data(db_cursor, all_city_data)
