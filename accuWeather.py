
import sqlite3
from datetime import datetime
from file_functions import get_api_data

def get_city_weather(city):
    api_token = "3c3f7d2d9f242452b9c1389c3172c412"
    request_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_token}"
    return get_api_data(request_url)

def create_weather_table(db_cursor):
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        temperature REAL,
        weather_description TEXT,
        timestamp TEXT,
        humidity INTEGER,
        wind_speed REAL,
        UNIQUE(city, timestamp)  -- Ensuring no duplicates
    )
    ''')

def insert_weather_data(db_cursor, city_data):
    """
    Insert weather data for a city into the database.
    """
    city_name = city_data['name']
    temp = city_data['main']['temp']  # Temperature in Kelvin
    weather_description = city_data['weather'][0]['description']
    humidity = city_data['main']['humidity']
    wind_speed = city_data['wind']['speed']
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        db_cursor.execute('''
        INSERT OR IGNORE INTO city_weather (city, temperature, weather_description, timestamp, humidity, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (city_name, temp, weather_description, timestamp, humidity, wind_speed))
    except sqlite3.Error as e:
        print(f"Error inserting data: {e}")

def get_multiple_city_weather(city_requests, db_cursor):

    # get weather data for multiple cities and insert it into the database.
   
    for city in city_requests:
        print(f"Fetching weather data for {city}...")
        city_data = get_city_weather(city)

        if city_data and city_data.get('cod') == 200:  # Status 200 means successful
            insert_weather_data(db_cursor, city_data)
            print(f"Data for {city} inserted into database.")
        else:
            print(f"Failed to retrieve data for {city}. Error: {city_data.get('message', 'Unknown error')}")

def count_rows_in_db(db_cursor):

    # Count the number of rows in the city_weather table.

    db_cursor.execute("SELECT COUNT(*) FROM city_weather")
    count = db_cursor.fetchone()[0]
    return count

def main():
    city_requests = [
        "New York", "London", "Tokyo", "Delhi", "Shanghai", "Sydney", 
        "Paris", "Berlin", "Cairo", "Moscow", "Dubai", "San Francisco", 
        "São Paulo", "Mumbai", "Los Angeles", "Mexico City", "Seoul", "Istanbul"
    ]

    # Remove duplicates
    city_requests = list(set(city_requests))

    with sqlite3.connect('global_weather.db') as conn:
        cursor = conn.cursor()
        create_weather_table(cursor)
        existing_count = count_rows_in_db(cursor)
        print(f"Existing rows in database: {existing_count}")
        get_multiple_city_weather(city_requests, cursor)
        conn.commit()
        updated_count = count_rows_in_db(cursor)
        print(f"Total rows in the database after insertion: {updated_count}")

if __name__ == "__main__":
    main()