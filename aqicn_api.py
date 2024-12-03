import sqlite3
from datetime import datetime
from file_functions import get_api_data

def get_city_aqi(city):
    api_token = "2f4c73a57b156c067a7fb45fa17784bd26cd0a8f"
    request_url = f'https://api.waqi.info/feed/{city}/?token={api_token}'
    return get_api_data(request_url)

def create_aqi_table(db_cursor):
    db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_aqi (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        aqi INTEGER,
        timestamp TEXT,
        dominant_pollutant TEXT,
        forecast_pm25_avg REAL,
        forecast_pm10_avg REAL,
        forecast_o3_avg REAL,
        UNIQUE(city, timestamp)  -- Ensuring no duplicates
    )
    ''')

def insert_aqi_data(db_cursor, city_data):
    city_name = city_data['data']['city']['name']
    aqi = city_data['data']['aqi']

    forecast_day = city_data['data']['forecast']['daily']['pm25'][0].get('day', 'unknown')  # Default to 'unknown' if 'day' is missing
    dominant_pollutant = city_data['data'].get('dominentpol', 'N/A')
    forecast_pm25 = city_data['data']['forecast']['daily']['pm25'][0]
    forecast_pm25_avg = forecast_pm25.get('avg', 0)
    forecast_pm10 = city_data['data']['forecast']['daily']['pm10'][0]
    forecast_pm10_avg = forecast_pm10.get('avg', 0) 
    forecast_o3 = city_data['data']['forecast']['daily']['o3'][0]
    forecast_o3_avg = forecast_o3.get('avg', 0)
    
    try:
        # Insert data into the database
        db_cursor.execute('''
        INSERT OR IGNORE INTO city_aqi (city, aqi, timestamp, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (city_name, aqi, forecast_day, dominant_pollutant, forecast_pm25_avg, forecast_pm10_avg, forecast_o3_avg))
    except sqlite3.Error as e:
        print(f"Error inserting data: {e}")

def get_multiple_city_aqi(city_requests, db_cursor):
    for city in city_requests:
        print(f"Fetching AQI data for {city}...")
        city_data = get_city_aqi(city)
        
        if city_data and city_data.get('status') == 'ok':
            insert_aqi_data(db_cursor, city_data)
            print(f"Data for {city} inserted into database.")
        else:
            print(f"Failed to retrieve data for {city}.")

def count_rows_in_db(db_cursor):
    db_cursor.execute("SELECT COUNT(*) FROM city_aqi")
    count = db_cursor.fetchone()[0]
    return count

def main():
    city_requests = [
        "Berlin", "Cairo", "São Paulo", "Tokyo", "Ann Arbor", "Sydney",
        "New York", "Los Angeles", "London", "Paris", "Mumbai", "Mexico City",
        "Moscow", "Shanghai", "Beijing", "Singapore", "Cape Town", "Toronto",
        "Hong Kong", "Dubai", "Rome", "Madrid", "San Francisco", "Los Angeles",
        "Seoul", "Istanbul", "Sydney", "Buenos Aires", "Lagos", "Rio de Janeiro",
        "Karachi", "Jakarta", "Kuala Lumpur", "Bangkok", "Delhi", "Lahore", 
        "Jakarta", "Athens", "Vienna", "Vancouver", "Brisbane", "Melbourne", 
        "Montreal", "Boston", "Chicago", "Copenhagen", "Oslo", 
        "Stockholm", "Helsinki", "Zurich", "Geneva", "Milan", "Amsterdam", 
        "Paris", "Dublin", "Osaka", "Seoul", "Beijing", "Cape Town", 
        "Lagos", "Manila", "Singapore", "Tel Aviv", "Auckland", "Lisbon", 
        "Montreal", "Miami", "Tbilisi", "Bucharest", "Warsaw", "Prague", 
        "Hong Kong", "Bangkok", "Delhi", "Kyiv", "Algiers", "Zagreb", "Vienna", 
        "Chennai", "Dhaka", "Lima", "Quito", "Addis Ababa", "Abu Dhabi", 
        "Mexico City", "Cairo", "Dubai", "Mumbai", "Kolkata", "Chengdu", 
        "Tianjin", "Suzhou", "Qingdao", "Guangzhou", "Shenzhen", "Hangzhou", 
        "Tianjin", "Nairobi", "Port Moresby", "Abuja", "Accra", "Khartoum", 
        "Marrakesh", "Algiers", "Dhaka", "Lagos", "Calcutta", "Jakarta", 
        "Hanoi", "Manama", "Pune", "Chennai", "Kathmandu", "Ulaanbaatar", 
        "Addis Ababa", "Almaty", "Cebu", "Porto", "Douala", "San Salvador", 
        "Medellín", "Lagos", "Calabar", "Zanzibar", "Accra", "Kinshasa", 
        "Dakar", "Monrovia", "Santiago", "Bogotá", "Guadalajara", "Lima", 
        "Rio de Janeiro", "Santiago", "Tijuana", "Cartagena", "Buenos Aires",
        "Tashkent", "Baku", "Vilnius", "Riga", "Colombo", 
        "San José", "San Salvador", "Cape Town", "Abuja", "Brasilia", "Jakarta"
    ]

    # Remove duplicates
    city_requests = list(set(city_requests))
    with sqlite3.connect('aqi_data.db') as conn:
        cursor = conn.cursor()
        create_aqi_table(cursor)
        existing_count = count_rows_in_db(cursor)
        print(f"Existing rows in database: {existing_count}")
        get_multiple_city_aqi(city_requests, cursor)
        conn.commit()
        updated_count = count_rows_in_db(cursor)
        print(f"Total rows in the database after insertion: {updated_count}")

if __name__ == "__main__":
    main()
