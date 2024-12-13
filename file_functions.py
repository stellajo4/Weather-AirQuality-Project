import requests
import sqlite3

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

# Create the pollutants and weather_conditions tables, and the city_aqi_data, city_weather_data tables
def create_combined_tables(db_cursor):
    """Create the pollutants, weather_conditions, city_aqi_data, and city_weather_data tables."""
    
    # Create the pollutants lookup table (mapping pollutant names to keys)
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS pollutants (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    ''')

    # Insert the pollutant types into the pollutants table if they don't exist
    pollutants = ["pm25", "pm10", "o3"]
    for pollutant in pollutants:
        db_cursor.execute('''
        INSERT OR IGNORE INTO pollutants (name) VALUES (?)
        ''', (pollutant,))
    
    # Create the weather_conditions lookup table (mapping weather descriptions to keys)
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_conditions (
        id INTEGER PRIMARY KEY,
        description TEXT UNIQUE
    )
    ''')

    # Insert the weather descriptions into the weather_conditions table if they don't exist
    weather_descriptions = {
        "clear": 1, "haze": 2, "overcast": 3, "mist": 4, "clear sky": 5,
        "few clouds": 6, "overcast clouds": 7, "smoke": 8, "broken clouds": 9
    }
    
    for description, description_id in weather_descriptions.items():
        db_cursor.execute('''
        INSERT OR IGNORE INTO weather_conditions (id, description) VALUES (?, ?)
        ''', (description_id, description))

    # Create the city_aqi_data table with a foreign key reference to pollutants
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_aqi_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        aqi INTEGER,
        timestamp TEXT,
        dominant_pollutant INTEGER,
        forecasted_pm25_avg REAL,
        forecasted_pm10_avg REAL,
        forecasted_o3_avg REAL,
        FOREIGN KEY(dominant_pollutant) REFERENCES pollutants(id)
    )
    ''')

    # Create the city_weather_data table with a foreign key reference to weather_conditions
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        timestamp TEXT,
        temperature REAL,
        weather_description INTEGER,
        humidity INTEGER,
        wind_speed REAL,
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
        timestamp = city_data['timestamp']  # Using timestamp from AQI data
        
        # Step 1: Fetch the pollutant key (1 for pm25, 2 for pm10, 3 for o3)
        dominant_pollutant_name = city_data['dominant_pollutant']
        db_cursor.execute('''
        SELECT id FROM pollutants WHERE name = ?
        ''', (dominant_pollutant_name,))
        dominant_pollutant_id = db_cursor.fetchone()
        
        if dominant_pollutant_id:
            dominant_pollutant_id = dominant_pollutant_id[0]
        else:
            dominant_pollutant_id = None  # Default or error handling if pollutant not found
        
        # Step 2: Insert the AQI data into city_aqi_data table
        db_cursor.execute('''
        INSERT INTO city_aqi_data (city, aqi, timestamp, dominant_pollutant, forecasted_pm25_avg, 
            forecasted_pm10_avg, forecasted_o3_avg)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            city_data['city'], city_data['aqi'], timestamp, dominant_pollutant_id,
            city_data['forecasted_pm25_avg'], city_data['forecasted_pm10_avg'], city_data['forecasted_o3_avg']
        ))
        
        # Step 3: Fetch the weather description key (1 for clear, 2 for haze, etc.)
        weather_description = weather_data['weather_description']
        db_cursor.execute('''
        SELECT id FROM weather_conditions WHERE description = ?
        ''', (weather_description,))
        weather_description_id = db_cursor.fetchone()

        if weather_description_id:
            weather_description_id = weather_description_id[0]
        else:
            weather_description_id = None  # Default or error handling if description not found
        
        # Step 4: Insert the weather data into city_weather_data table
        db_cursor.execute('''
        INSERT INTO city_weather_data (city, timestamp, temperature, weather_description, humidity, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            weather_data['city'], weather_data['timestamp'], weather_data['temperature'],
            weather_description_id, weather_data['humidity'], weather_data['wind_speed']
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
            'dominant_pollutant': aqi_data['data'].get('dominentpol', 'pm25'),  # Default to pm25 if not available
            'forecasted_pm25_avg': aqi_data['data']['forecast']['daily']['pm25'][0].get('avg', 0),
            'forecasted_pm10_avg': aqi_data['data']['forecast']['daily']['pm10'][0].get('avg', 0),
            'forecasted_o3_avg': aqi_data['data']['forecast']['daily']['o3'][0].get('avg', 0)
        }
    else:
        print(f"Failed to retrieve AQI data for {city}.")
        return None

    # Fetch weather data and ensure the city field is included in the weather data
    weather_data = get_weather_data_for_city(city)
    if weather_data and weather_data.get('cod') == 200:
        weather_info = {
            'city': city,  # Add the city here
            'timestamp': weather_data['dt'],  # Assuming the timestamp is available in 'dt'
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
def get_multiple_city_combined_data(city_requests, db_cursor):
    """Retrieve and insert combined AQI and weather data for multiple cities, limiting to 25 each time."""
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

def create_avg_temperature_table(db_cursor):
    """Create the table to store average temperatures."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_avg_temperature (
        city TEXT PRIMARY KEY,
        avg_temperature REAL
    )
    ''')
    db_cursor.connection.commit()  # Ensure changes are committed
    print("Table city_avg_temperature created or already exists.")

# Example main function to run the entire process
def main():
    city_requests = [
        "New York", "London", "Tokyo", "Delhi", "Shanghai", "Sydney", "Paris", "Berlin", "Cairo", "Moscow", 
        "Dubai", "San Francisco", "São Paulo", "Mumbai", "Los Angeles", "Mexico City", "Seoul", "Istanbul", "Hong Kong", "Lagos"
    ]
    
    with sqlite3.connect('global_combined_data.db') as conn:  # Unified database
        cursor = conn.cursor()
        
        # Create necessary tables (pollutants, weather_conditions, city_aqi_data, and city_weather_data)
        create_combined_tables(cursor)
        
        # Retrieve and insert combined AQI and weather data for the specified cities
        get_multiple_city_combined_data(city_requests, cursor)
        
        conn.commit()

if __name__ == "__main__":
    main()
