import sqlite3
import requests
from file_functions import get_api_data

def get_city_aqi(city):
    api_token = "2f4c73a57b156c067a7fb45fa17784bd26cd0a8f"
    request_url = f'https://api.waqi.info/feed/{city}/?token={api_token}'
    return get_api_data(request_url)

def get_api_data(request_url):
    """Simulate getting data from API (you need to implement this or use a real API call)"""
    try:
        response = requests.get(request_url)
        if response.status_code == 200:
            return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
    return {}

def create_aqi_table(db_cursor):
    # Create the main AQI table
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_aqi (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        aqi INTEGER,
        timestamp TEXT,
        dominant_pollutant INTEGER,
        forecast_pm25_avg REAL,
        forecast_pm10_avg REAL,
        forecast_o3_avg REAL,
        UNIQUE(city, timestamp)  -- Ensuring no duplicates
    )
    ''')

def create_pollutant_key_table(db_cursor):
    # Create the pollutant key table that maps pollutant names to numeric values
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS pollutant_key (
        pollutant TEXT PRIMARY KEY,
        value INTEGER
    )
    ''')

    # Insert the key values for pm25, o3, and pm10
    db_cursor.executemany('''
    INSERT OR IGNORE INTO pollutant_key (pollutant, value)
    VALUES (?, ?)
    ''', [
        ('pm25', 1),
        ('o3', 2),
        ('pm10', 3)
    ])

def get_pollutant_value(db_cursor, pollutant):
    # Fetch the numeric value for a given pollutant from the pollutant_key table
    db_cursor.execute('SELECT value FROM pollutant_key WHERE pollutant = ?', (pollutant,))
    result = db_cursor.fetchone()
    return result[0] if result else None

def insert_aqi_data(db_cursor, city_data_list):
    """
    Insert a list of city AQI data into the database in chunks, checking for duplicates before insertion.
    """
    for city_data in city_data_list:
        city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg = city_data
        
        # Check if the city and timestamp already exist in the table
        db_cursor.execute('''
        SELECT 1 FROM city_aqi WHERE city = ? AND timestamp = ?
        ''', (city, timestamp))
        
        if db_cursor.fetchone():
            print(f"Skipping {city} for timestamp {timestamp}: Data already exists.")
        else:
            try:
                db_cursor.execute('''
                INSERT OR IGNORE INTO city_aqi (city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', city_data)
                print(f"Inserted {city} data for timestamp {timestamp}.")
            except sqlite3.Error as e:
                print(f"Error inserting data: {e}")

    db_cursor.connection.commit()  # Commit after each batch

def get_multiple_city_aqi(city_requests, db_cursor):
    # Read progress from the database
    start_index = read_progress(db_cursor)
    print(f"Starting from city index: {start_index}")

    all_city_data = []
    for i, city in enumerate(city_requests[start_index:], start=start_index):
        if len(all_city_data) >= 25:  # Stop after adding 25 cities
            break

        print(f"Fetching AQI data for {city}...")
        city_data = get_city_aqi(city)

        if city_data and city_data.get('status') == 'ok':
            city_name = city  # Ensure the name is as inputted
            aqi = city_data['data']['aqi']
            forecast_day = city_data['data']['forecast']['daily']['pm25'][0].get('day', 'unknown')
            
            dominant_pollutant = city_data['data'].get('dominentpol', 'N/A')
            # Lookup the numeric value for the dominant pollutant
            dominant_pollutant_value = get_pollutant_value(db_cursor, dominant_pollutant)

            forecast_pm25 = city_data['data']['forecast']['daily']['pm25'][0]
            forecast_pm25_avg = forecast_pm25.get('avg', 0)
            forecast_pm10 = city_data['data']['forecast']['daily']['pm10'][0]
            forecast_pm10_avg = forecast_pm10.get('avg', 0)
            forecast_o3 = city_data['data']['forecast']['daily']['o3'][0]
            forecast_o3_avg = forecast_o3.get('avg', 0)

            city_data_tuple = (
                city_name, aqi, forecast_day, dominant_pollutant_value,
                forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg
            )
            all_city_data.append(city_data_tuple)

        else:
            print(f"Failed to retrieve data for {city}.")

    # Insert data for the 25 cities
    if all_city_data:
        insert_aqi_data(db_cursor, all_city_data)

    # Update progress after inserting 25 cities
    update_progress(db_cursor, i + 1)

def read_progress(db_cursor):
    """Read the index of the last processed city from the database"""
    db_cursor.execute('SELECT value FROM progress WHERE key = "last_processed_index"')
    result = db_cursor.fetchone()
    return result[0] if result else 0  # Start from the beginning if no progress found

def update_progress(db_cursor, index):
    """Update the progress in the database"""
    db_cursor.execute('''
    INSERT OR REPLACE INTO progress (key, value)
    VALUES ("last_processed_index", ?)
    ''', (index,))
    db_cursor.connection.commit()
    print(f"Progress saved at city index: {index}")

def create_progress_table(db_cursor):
    """Create a table to store progress information"""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS progress (
        key TEXT PRIMARY KEY,
        value INTEGER
    )
    ''')

def count_rows_in_db(db_cursor):
    db_cursor.execute("SELECT COUNT(*) FROM city_aqi")
    count = db_cursor.fetchone()[0]
    return count

def main():
    city_requests = [
        "New York", "London", "Tokyo", "Delhi", "Shanghai", "Sydney", "Paris", "Berlin", "Cairo", "Moscow",
        "Dubai", "San Francisco", "São Paulo", "Mumbai", "Los Angeles", "Mexico City", "Seoul", "Istanbul", "Hong Kong", "Lagos",
        "Bangkok", "Rome", "Jakarta", "Kuala Lumpur", "Singapore", "Riyadh", "Manila", "Karachi", "Madrid",
        "Chennai", "Melbourne", "Toronto", "Vienna", "Cape Town", "Montreal", "Rio de Janeiro", "Santiago", "Lima", "Vancouver",
        "Buenos Aires", "Bangalore", "Oslo", "Dubai", "Kolkata", "Tehran", "Zurich", "Copenhagen", "Auckland", "Hong Kong",
        "Berlin", "Barcelona", "Vienna", "Zurich", "Madrid", "Amsterdam", "Brisbane", "Stockholm", "Budapest", "Lagos",
        "Bucharest", "Jakarta", "Dublin", "Toronto", "Vancouver", "Washington, D.C.", "Seoul", "Stockholm", "Athens", "Dubai",
        "Milan", "Bangkok", "San Diego", "Helsinki", "Oslo", "Kiev", "Cairo", "Warsaw", "Manila", "Lagos",
        "Miami", "Kuwait City", "Baku", "Cape Town", "Accra", "Johannesburg", "Portland", "Frankfurt", "Lisbon", "Paris",
        "Cape Town", "Colombo", "Algiers", "Seoul", "Stockholm", "Athens", "Porto", "Vienna", "Hong Kong", "Baku",
        "Helsinki", "Lagos", "Kigali", "Sarajevo", "Bratislava", "Warsaw", "Nairobi", "Auckland", "Toronto", "Cairo",
        "Paris", "Kuala Lumpur", "Lagos", "Chennai", "Jakarta", "Los Angeles", "Rio de Janeiro", "Santiago", "Lagos", "Düsseldorf"
    ]

    with sqlite3.connect('aqi_data.db') as conn:
        cursor = conn.cursor()
        create_progress_table(cursor)  # Create the progress table
        create_pollutant_key_table(cursor)  # Create the pollutant key table
        create_aqi_table(cursor)  # Create the main AQI table
        existing_count = count_rows_in_db(cursor)
        print(f"Existing rows in database: {existing_count}")
        get_multiple_city_aqi(city_requests, cursor)
        conn.commit()

if __name__ == "__main__":
    main()
