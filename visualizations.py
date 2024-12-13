import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the SQLite database
def connect_to_db(db_name='global_combined_data.db'):
    """Connect to the SQLite database."""
    conn = sqlite3.connect(db_name)
    return conn

# Check city_aqi_data table
def check_aqi_data():
    """Check the first few rows of the city_aqi_data table."""
    conn = connect_to_db()
    query = "SELECT * FROM city_aqi_data LIMIT 10;"
    df = pd.read_sql(query, conn)
    conn.close()
    print("city_aqi_data")
    print(df)

# Check city_weather_data table
def check_weather_data():
    """Check the first few rows of the city_weather_data table."""
    conn = connect_to_db()
    query = "SELECT * FROM city_weather_data LIMIT 10;"
    df = pd.read_sql(query, conn)
    conn.close()
    print("city_weather_data")
    print(df)

# Fetch the joined data using the SQL query
def fetch_joined_data():
    """Fetch joined AQI and weather data from both tables."""
    conn = connect_to_db()
    query = '''
    SELECT DISTINCT
        aqi.city,
        aqi.timestamp,
        aqi.aqi,
        aqi.forecasted_pm25_avg,
        aqi.forecasted_pm10_avg,
        aqi.forecasted_o3_avg,
        weather.temperature,
        weather.humidity,
        weather.wind_speed
    FROM
        city_aqi_data AS aqi
    JOIN
        city_weather_data AS weather
    ON
        aqi.city = weather.city
    AND
        aqi.timestamp = weather.timestamp
    WHERE
        aqi.aqi IS NOT NULL
    AND
        weather.temperature IS NOT NULL
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    print("Joined data:")
    print(df.head())  # Print the first few rows to see if data is being returned
    return df

# Create a scatter plot to visualize the relationship between AQI and temperature
def plot_aqi_vs_temperature(df):
    """Plot AQI vs Temperature and save as PNG."""
    plt.figure(figsize=(10, 6))
    plt.scatter(df['temperature'], df['aqi'], alpha=0.7, c=df['aqi'], cmap='viridis')
    plt.title('AQI vs Temperature')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('AQI')
    plt.colorbar(label='AQI')
    plt.grid(True)
    plt.savefig('aqi_vs_temperature.png')  # Save the plot as a PNG image
    plt.close()  # Close the plot to avoid display in non-interactive environments

# Create a line plot to visualize the relationship between AQI and humidity
def plot_aqi_vs_humidity(df):
    """Plot AQI vs Humidity and save as PNG."""
    plt.figure(figsize=(10, 6))
    plt.plot(df['humidity'], df['aqi'], marker='o', linestyle='-', color='b')
    plt.title('AQI vs Humidity')
    plt.xlabel('Humidity (%)')
    plt.ylabel('AQI')
    plt.grid(True)
    plt.savefig('aqi_vs_humidity.png')  # Save the plot as a PNG image
    plt.close()

# Create a line plot to visualize the relationship between AQI and wind speed
def plot_aqi_vs_wind_speed(df):
    """Plot AQI vs Wind Speed and save as PNG."""
    plt.figure(figsize=(10, 6))
    plt.plot(df['wind_speed'], df['aqi'], marker='x', linestyle='-', color='r')
    plt.title('AQI vs Wind Speed')
    plt.xlabel('Wind Speed (m/s)')
    plt.ylabel('AQI')
    plt.grid(True)
    plt.savefig('aqi_vs_wind_speed.png')  # Save the plot as a PNG image
    plt.close()

# Main function to run the entire process
def main():
    """Main function to check data, fetch the joined data, and create plots."""
    
    # Check the data in the tables
    print("Checking data in city_aqi_data table...")
    check_aqi_data()
    
    print("Checking data in city_weather_data table...")
    check_weather_data()

    # Fetch the joined data from the database
    df = fetch_joined_data()

    # Visualize the relationship between AQI and Temperature
    plot_aqi_vs_temperature(df)

    # Visualize the relationship between AQI and Humidity
    plot_aqi_vs_humidity(df)

    # Visualize the relationship between AQI and Wind Speed
    plot_aqi_vs_wind_speed(df)

if __name__ == '__main__':
    main()
