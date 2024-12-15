import sqlite3
from file_functions import get_api_data
from datetime import datetime

def get_city_aqi(city):
    """Get AQI data for a city."""
    api_url = f'https://api.waqi.info/feed/{city}/?token=2f4c73a57b156c067a7fb45fa17784bd26cd0a8f'
    return get_api_data(api_url)

def create_progress_table(db_cursor):
    """Create a table to store progress information."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS progress (
        key TEXT PRIMARY KEY,
        value INTEGER
    )
    ''')

def read_progress(db_cursor):
    """Read the index of the last processed city from the database."""
    db_cursor.execute('SELECT value FROM progress WHERE key = "last_processed_index"')
    result = db_cursor.fetchone()
    return result[0] if result else 0 

def update_progress(db_cursor, index):
    """Update the progress in the database."""
    db_cursor.execute('''
    INSERT OR REPLACE INTO progress (key, value)
    VALUES ("last_processed_index", ?)
    ''', (index,))
    db_cursor.connection.commit()
    print(f"Progress saved at city index: {index}")

def create_aqi_table(db_cursor):
    """Create the AQI table in the database."""
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
        UNIQUE(city, timestamp)
    )
    ''')

def create_pollutant_key_table(db_cursor):
    """Create a table for pollutant types."""
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS pollutant_key (
        pollutant TEXT PRIMARY KEY,
        value INTEGER
    )
    ''')

    db_cursor.executemany('''
    INSERT OR IGNORE INTO pollutant_key (pollutant, value)
    VALUES (?, ?)
    ''', [
        ('pm25', 1),
        ('o3', 2),
        ('pm10', 3)
    ])

def get_pollutant_value(db_cursor, pollutant):
    """Retrieve the numeric value for a given pollutant from the pollutant_key table."""
    db_cursor.execute('SELECT value FROM pollutant_key WHERE pollutant = ?', (pollutant,))
    result = db_cursor.fetchone()
    return result[0] if result else None

def get_multiple_city_aqi(city_requests, db_cursor):
    """Retrieve and insert AQI data for multiple cities in batches of 25."""
    start_index = read_progress(db_cursor)
    print(f"Starting from city index: {start_index}")
    
    all_city_data = []
    
    for i, city in enumerate(city_requests[start_index:], start=start_index):
        if len(all_city_data) >= 25:  # Process only 25 cities at a time
            break
        
        print(f"Fetching AQI data for {city}...")
        city_data = get_api_data(f'https://api.waqi.info/feed/{city}/?token=2f4c73a57b156c067a7fb45fa17784bd26cd0a8f')

        if city_data and city_data.get('status') == 'ok':
            city_name = city
            aqi = city_data['data']['aqi']
            dominant_pollutant = city_data['data'].get('dominentpol', 'N/A')
            dominant_pollutant_value = get_pollutant_value(db_cursor, dominant_pollutant)

            # Set timestamp to the current time
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            forecast_pm25 = city_data['data']['forecast']['daily']['pm25'][0]
            forecast_pm25_avg = forecast_pm25.get('avg', 0)
            forecast_pm10 = city_data['data']['forecast']['daily']['pm10'][0]
            forecast_pm10_avg = forecast_pm10.get('avg', 0)
            forecast_o3 = city_data['data']['forecast']['daily']['o3'][0]
            forecast_o3_avg = forecast_o3.get('avg', 0)
            
            city_data_tuple = (
                city_name, aqi, timestamp, dominant_pollutant_value,
                forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg
            )
            all_city_data.append(city_data_tuple)
        else:
            print(f"Failed to retrieve data for {city}.")
    
    if all_city_data:
        insert_aqi_data(db_cursor, all_city_data)
    
    # Update progress after batch processing
    update_progress(db_cursor, i + 1)

def insert_aqi_data(db_cursor, city_data_list):
    """Insert AQI data for a list of cities."""
    for city_data in city_data_list:
        city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg = city_data

        db_cursor.execute('''
        SELECT 1 FROM city_aqi WHERE city = ? AND timestamp = ?
        ''', (city, timestamp))
        
        if db_cursor.fetchone():
            print(f"Skipping {city} for timestamp {timestamp}: Data already exists.")
        else:
            db_cursor.execute('''
            INSERT OR IGNORE INTO city_aqi (city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg))
            print(f"Inserted {city} data for timestamp {timestamp}.")
    
    db_cursor.connection.commit()
