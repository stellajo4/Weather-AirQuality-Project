def calculate_and_store_average_temperature_for_pollutant_1(cursor):
    """Calculate and store the average temperature for cities with dominant pollutant = 1 (pm25)."""
    
    # Query to find cities with dominant pollutant 1 (pm25) in the city_aqi_data table
    cursor.execute('''
    SELECT city FROM city_aqi_data WHERE dominant_pollutant = 1
    ''')
    
    # Fetch all cities with dominant pollutant 1
    cities_with_pollutant_1 = cursor.fetchall()

    if not cities_with_pollutant_1:
        print("No cities with pollutant 1 found.")
        return
    
    for city_tuple in cities_with_pollutant_1:
        city = city_tuple[0]

        # Calculate average temperature for each city with pollutant 1 from the city_weather_data table
        cursor.execute('''
        SELECT AVG(temperature) FROM city_weather_data WHERE city = ?
        ''', (city,))
        
        average_temperature = cursor.fetchone()[0]

        if average_temperature is not None:
            print(f"Average temperature for {city} (with pollutant 1) is {average_temperature}°C.")
            
            # Store the average temperature (or update if already exists) in city_avg_temperature table
            cursor.execute('''
            INSERT OR REPLACE INTO city_avg_temperature (city, avg_temperature)
            VALUES (?, ?)
            ''', (city, average_temperature))
        
        else:
            print(f"No temperature data found for {city}.")
