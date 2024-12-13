import sqlite3
from file_functions import create_combined_table, get_multiple_city_combined_data

def main():
    city_requests = [
        "New York", "London", "Tokyo", "Delhi", "Shanghai", "Sydney", "Paris", "Berlin", "Cairo", "Moscow", 
        "Dubai", "San Francisco", "São Paulo", "Mumbai", "Los Angeles", "Mexico City", "Seoul", "Istanbul", "Hong Kong", "Lagos"
    ]

    with sqlite3.connect('global_combined_data.db') as conn:  # Unified database
        cursor = conn.cursor()
        create_combined_table(cursor)
        get_multiple_city_combined_data(city_requests, cursor)
        conn.commit()

if __name__ == "__main__":
    main()