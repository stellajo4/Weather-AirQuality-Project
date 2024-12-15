import sqlite3
from file_functions import create_combined_tables, get_multiple_city_combined_data, create_avg_temperature_table
from calculations import calculate_and_store_average_temperature_for_pollutant_1
import visualizations

def show_table(output_file):
    """Display the city_avg_temperature table."""
    connection = sqlite3.connect('global_combined_data.db')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM city_avg_temperature")
    rows = cursor.fetchall()

    with open(output_file, "a") as file:
        file.write("\nCity Average Temperatures Table:\n")
        for row in rows:
            file.write(f"{row}\n")
    connection.close()

def fetch_average_temperatures(output_file):
    """Fetch and display average temperatures."""
    connection = sqlite3.connect('global_combined_data.db')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM city_avg_temperature;")
    rows = cursor.fetchall()

    with open(output_file, "a") as file:
        file.write("\nAverage Temperatures for Cities:\n")
        for row in rows:
            file.write(f"{row}\n")
    connection.close()

def main():
    output_file = "average_temperature_results.txt"
    city_requests = [
        "New York", "Tokyo", "Paris", "London", "Sydney", "Berlin", "Rome", "Madrid", "Moscow", "Dubai",
        "Los Angeles", "Shanghai", "São Paulo", "Cairo", "Buenos Aires", "Bangkok", "Istanbul", "Singapore", "Toronto", "Mexico City",
        "Hong Kong", "Seoul", "Lagos", "Cape Town", "Mumbai", "Rio de Janeiro", "Kuala Lumpur", "Beijing", "Vienna", "Vienna",
        "Jakarta", "Lagos", "Melbourne", "Karachi", "San Francisco", "Dubai", "Lima", "Jakarta", "Manila", "Rio de Janeiro",
        "Lagos", "Cape Town", "Oslo", "Geneva", "Madrid", "Paris", "Berlin", "Helsinki", "Kiev", "Athens", "Lisbon", "Rome", "Amsterdam",
        "Hong Kong", "Bangkok", "Bangladesh", "Dubai", "Toronto", "Los Angeles", "Chicago", "London", "Dubai", "Cairo", "Singapore", "Mumbai",
        "Toronto", "Sydney", "Milan", "Rome", "Barcelona", "Mexico City", "Vienna", "Shanghai", "Istanbul", "Seoul", "Bangkok", "Cape Town", 
        "Berlin", "Tokyo", "Buenos Aires", "Tokyo", "San Francisco", "Amsterdam", "Sao Paulo", "Dublin", "Mexico City", "Moscow", "Jakarta",
        "San Diego", "Kuala Lumpur", "Santiago", "Helsinki", "Moscow", "Sydney", "Milan", "Manila", "Karachi", "Lagos", "Mumbai", "San Francisco", 
        "Paris", "Los Angeles", "Madrid", "Berlin", "Rome", "Toronto", "Cairo", "Beijing", "Istanbul", "Bangkok", "Singapore", "Rio de Janeiro", 
        "Jakarta", "Melbourne", "Mumbai", "Bangalore", "Vienna", "Seoul", "Oslo", "Berlin", "Geneva", "Barcelona", "Shanghai", "Rome", "London", 
        "Paris", "Berlin", "Cairo", "Sydney", "Lagos", "Tokyo", "Mexico City", "San Francisco", "Düsseldorf", "Singapore", "Lisbon", "Athens", 
        "Bangkok", "Sao Paulo", "Kuala Lumpur", "New York", "Dubai", "Moscow", "Rome", "Beijing", "Toronto", "Mexico City", "Cape Town", 
        "Kiev", "Paris", "Los Angeles", "Istanbul", "Barcelona", "Dubai", "San Francisco", "Melbourne", "Moscow", "Buenos Aires", "Berlin", 
        "London", "Madrid", "Shanghai", "Bangalore", "Rio de Janeiro", "Seoul", "Lagos", "Jakarta", "Berlin", "Rome", "Mumbai", "Sydney", 
        "Cape Town", "Paris", "Vienna", "Helsinki", "Kuala Lumpur", "London", "Santiago", "Los Angeles", "Toronto", "San Francisco", "Istanbul"
    ]

    
    with sqlite3.connect('global_combined_data.db') as conn:
        cursor = conn.cursor()
        create_combined_tables(cursor)
        with open(output_file, "w") as file:
            file.write("Creating table for average temperatures...\n")
        
        create_avg_temperature_table(cursor)
        with open(output_file, "a") as file:
            file.write("Fetching and inserting data for cities in batches...\n")
        get_multiple_city_combined_data(city_requests, cursor, batch_size=25)

        with open(output_file, "a") as file:
            file.write("Calculating and storing average temperatures for cities with dominant_pollutant = 1...\n")
        calculate_and_store_average_temperature_for_pollutant_1(cursor)
        fetch_average_temperatures(output_file)
        conn.commit()
    show_table(output_file)
    print(f"Process complete! Results are saved in {output_file}")
    #visualizations.main()

if __name__ == "__main__":
    main()