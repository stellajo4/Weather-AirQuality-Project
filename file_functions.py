import requests
import sqlite3
import os

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
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Create all tables without the timestamp column
def create_combined_tables(db_cursor):
    """Create the pollutants, weather_conditions, cities, city_aqi_data, and city_weather_data tables."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS pollutants (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    ''')

    pollutants = ["pm25", "pm10", "o3"]
    for pollutant in pollutants:
        db_cursor.execute('''
        INSERT OR IGNORE INTO pollutants (name) VALUES (?)
        ''', (pollutant,))

    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_conditions (
        id INTEGER PRIMARY KEY,
        description TEXT UNIQUE
    )
    ''')

    weather_descriptions = {
        "clear": 1, "haze": 2, "overcast": 3, "mist": 4, "clear sky": 5,
        "few clouds": 6, "overcast clouds": 7, "smoke": 8, "broken clouds": 9
    }

    for description, description_id in weather_descriptions.items():
        db_cursor.execute('''
        INSERT OR IGNORE INTO weather_conditions (id, description) VALUES (?, ?)
        ''', (description_id, description))

    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS cities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )
    ''')

    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_aqi_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER,
        aqi INTEGER,
        dominant_pollutant INTEGER,
        forecasted_pm25_avg REAL,
        forecasted_pm10_avg REAL,
        forecasted_o3_avg REAL,
        FOREIGN KEY(city_id) REFERENCES cities(id),
        FOREIGN KEY(dominant_pollutant) REFERENCES pollutants(id)
    )
    ''')

    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER,
        temperature REAL,
        weather_description INTEGER,
        humidity INTEGER,
        wind_speed REAL,
        FOREIGN KEY(city_id) REFERENCES cities(id),
        FOREIGN KEY(weather_description) REFERENCES weather_conditions(id)
    )
    ''')

    db_cursor.connection.commit()

# Insert combined AQI and weather data into the database
def insert_combined_data(db_cursor, combined_data):
    """Insert combined AQI and weather data into the database."""
    
    for data in combined_data:
        city_data = data['city_data']
        weather_data = data['weather_data']
        
        dominant_pollutant_name = city_data['dominant_pollutant']
        db_cursor.execute('''
        SELECT id FROM pollutants WHERE name = ?
        ''', (dominant_pollutant_name,))
        dominant_pollutant_id = db_cursor.fetchone()
        
        if dominant_pollutant_id:
            dominant_pollutant_id = dominant_pollutant_id[0]
        else:
            dominant_pollutant_id = None 
        
        db_cursor.execute('''
        INSERT INTO city_aqi_data (city_id, aqi, dominant_pollutant, forecasted_pm25_avg, 
            forecasted_pm10_avg, forecasted_o3_avg)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            city_data['city_id'], city_data['aqi'], dominant_pollutant_id,
            city_data['forecasted_pm25_avg'], city_data['forecasted_pm10_avg'], city_data['forecasted_o3_avg']
        ))
        
        weather_description = weather_data['weather_description']
        db_cursor.execute('''
        SELECT id FROM weather_conditions WHERE description = ?
        ''', (weather_description,))
        weather_description_id = db_cursor.fetchone()

        if weather_description_id:
            weather_description_id = weather_description_id[0]
        else:
            weather_description_id = None 

        db_cursor.execute('''
        INSERT INTO city_weather_data (city_id, temperature, weather_description, humidity, wind_speed)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            weather_data['city_id'], weather_data['temperature'],
            weather_description_id, weather_data['humidity'], weather_data['wind_speed']
        ))

    db_cursor.connection.commit()

# Fetch both AQI and weather data for a single city
def get_combined_city_data(city, db_cursor):
    db_cursor.execute('''
    INSERT OR IGNORE INTO cities (name) VALUES (?)
    ''', (city,))
    db_cursor.execute('''
    SELECT id FROM cities WHERE name = ?
    ''', (city,))
    city_id = db_cursor.fetchone()[0]
    
    aqi_data = get_aqi_data(city)
    if aqi_data and aqi_data.get('status') == 'ok':
        city_data = {
            'city_id': city_id,
            'aqi': aqi_data['data']['aqi'],
            'dominant_pollutant': aqi_data['data'].get('dominentpol', 'pm25'),
            'forecasted_pm25_avg': aqi_data['data']['forecast']['daily']['pm25'][0].get('avg', 0),
            'forecasted_pm10_avg': aqi_data['data']['forecast']['daily']['pm10'][0].get('avg', 0),
            'forecasted_o3_avg': aqi_data['data']['forecast']['daily']['o3'][0].get('avg', 0)
        }
    else:
        print(f"Failed to retrieve AQI data for {city}.")
        return None

    weather_data = get_weather_data_for_city(city)
    if weather_data and weather_data.get('cod') == 200:
        weather_info = {
            'city_id': city_id,  # Store city_id
            'temperature': weather_data['main']['temp'] - 273.15,  # Convert from Kelvin to Celsius
            'weather_description': weather_data['weather'][0]['description'],
            'humidity': weather_data['main']['humidity'],
            'wind_speed': weather_data['wind']['speed']
        }
    else:
        print(f"Failed to retrieve weather data for {city}.")
        return None
    return {'city_data': city_data, 'weather_data': weather_info}

# Retrieve and insert combined AQI and weather data for multiple cities (up to 25 at a time)
def get_multiple_city_combined_data(city_requests, db_cursor, batch_size=25):
    """Retrieve and insert combined AQI and weather data for multiple cities, processing them in batches."""
    start_index = get_last_processed_city_index()  # Get the last processed city index
    total_cities = len(city_requests)
    end_index = start_index + batch_size
    cities_to_process = city_requests[start_index:end_index]

    combined_data = []
    for city in cities_to_process:
        print(f"Fetching combined data for {city}...")
        data = get_combined_city_data(city, db_cursor)
        if data:
            combined_data.append(data)
        else:
            print(f"Skipping {city} due to data retrieval failure.")
    
    if combined_data:
        insert_combined_data(db_cursor, combined_data)
        set_last_processed_city_index(end_index)  # Update progress file to the next batch

    if end_index >= total_cities:
        print("All cities have been processed.")
    else:
        print(f"Processed cities up to {city_requests[end_index-1]}. Next batch will start from {city_requests[end_index]}.")

def create_avg_temperature_table(db_cursor):
    """Create the table to store average temperatures using city_id."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_avg_temperature (
        city_id INTEGER PRIMARY KEY,
        avg_temperature REAL,
        FOREIGN KEY(city_id) REFERENCES cities(id)
    )
    ''')
    db_cursor.connection.commit()
    print("Table city_avg_temperature created or already exists.")

def get_last_processed_city_index(progress_file="last_processed_city.txt"):
    """Retrieve the index of the last processed city from the file."""
    if os.path.exists(progress_file): # Got this structre from ChatGPT
        with open(progress_file, 'r') as f:
            try:
                return int(f.read().strip()) 
            except ValueError:
                return 0
    return 0  # Default to start from the beginning if the file doesn't exist

def set_last_processed_city_index(index, progress_file="last_processed_city.txt"):
    """Store the index of the last processed city into the file."""
    with open(progress_file, 'w') as f:
        f.write(str(index))