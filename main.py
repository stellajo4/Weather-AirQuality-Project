import sqlite3
from file_functions import create_combined_tables, get_multiple_city_combined_data, create_avg_temperature_table
from calculations import calculate_and_store_average_temperature_for_pollutant_1

def show_table(output_file):
    # Connect to your database (update the path if needed)
    connection = sqlite3.connect('/Users/hallewhitman/Desktop/SI206/si206/Weather-AirQuality-Project/Weather-AirQuality-Project/global_combined_data.db')
    cursor = connection.cursor()

    # Query to select everything from city_avg_temperature table
    cursor.execute("SELECT * FROM city_avg_temperature")

    # Fetch all the rows
    rows = cursor.fetchall()

    # Write the table contents to the output file
    with open(output_file, "a") as file:
        file.write("\nCity Average Temperatures Table:\n")
        for row in rows:
            file.write(f"{row}\n")

    # Close the connection
    connection.close()

def fetch_average_temperatures(output_file):
    # Connect to the database
    connection = sqlite3.connect('/Users/hallewhitman/Desktop/SI206/si206/Weather-AirQuality-Project/Weather-AirQuality-Project/global_combined_data.db') 
    cursor = connection.cursor()

    # Execute the query
    cursor.execute("SELECT * FROM city_avg_temperature;")

    # Fetch all the results
    rows = cursor.fetchall()

    # Write the results to the output file
    with open(output_file, "a") as file:
        file.write("\nAverage Temperatures for Cities:\n")
        for row in rows:
            file.write(f"{row}\n")

    # Close the connection
    connection.close()

def main():
    output_file = "average_temperature_results.txt"
    city_requests = [
        "New York", "London", "Tokyo", "Delhi", "Shanghai", "Sydney", "Paris", "Berlin", "Cairo", "Moscow", 
        "Dubai", "San Francisco", "São Paulo", "Mumbai", "Los Angeles", "Mexico City", "Seoul", "Istanbul", "Hong Kong", "Lagos"
    ]

    with sqlite3.connect('global_combined_data.db') as conn:  # Unified database
        cursor = conn.cursor()

        # Ensure the necessary tables are created
        create_combined_tables(cursor)

        # Retrieve and insert AQI and weather data for the specified cities
        get_multiple_city_combined_data(city_requests, cursor)

        # Create the table for storing average temperatures if it doesn't exist
        with open(output_file, "w") as file:
            file.write("Creating table for average temperatures...\n")
        
        create_avg_temperature_table(cursor)

        # Calculate and store the average temperatures for cities with pollutant 1 (pm25)
        with open(output_file, "a") as file:
            file.write("Calculating and storing average temperatures for cities with dominant_pollutant = 1...\n")
        
        calculate_and_store_average_temperature_for_pollutant_1(cursor)

        # Fetch and log the average temperatures from the database
        fetch_average_temperatures(output_file)

        conn.commit()

    # Show table contents
    show_table(output_file)
    print(f"Process complete! Results are saved in {output_file}")

if __name__ == "__main__":
    main()


