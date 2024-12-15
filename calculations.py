def calculate_and_store_average_temperature_for_pollutant_1(cursor):
    """Calculate and store the average temperature for cities with dominant pollutant = 1 (pm25)."""
    cursor.execute('''
    SELECT DISTINCT city_id FROM city_aqi_data WHERE dominant_pollutant = 1
    ''')
    city_ids_with_pollutant_1 = cursor.fetchall()

    if not city_ids_with_pollutant_1:
        print("No cities with pollutant 1 found.")
        return
    
    for city_id_tuple in city_ids_with_pollutant_1:
        city_id = city_id_tuple[0]
        cursor.execute('''
        SELECT AVG(temperature) FROM city_weather_data WHERE city_id = ?
        ''', (city_id,))
        
        average_temperature = cursor.fetchone()[0]

        if average_temperature is not None:
            cursor.execute('''
            SELECT name FROM cities WHERE id = ?
            ''', (city_id,))
            city_name = cursor.fetchone()[0]

            print(f"Average temperature for {city_name} (with pollutant 1) is {average_temperature}°C.")
            cursor.execute('''
            INSERT OR REPLACE INTO city_avg_temperature (city_id, avg_temperature)
            VALUES (?, ?)
            ''', (city_id, average_temperature))
        
        else:
            print(f"No temperature data found for city with ID {city_id}.")
