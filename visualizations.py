import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def connect_to_db(db_name='global_combined_data.db'):
    conn = sqlite3.connect(db_name)
    return conn

def fetch_avg_temperature_data(db_cursor):
    query = '''
    SELECT city_id, avg_temperature
    FROM city_avg_temperature
    '''
    df = pd.read_sql_query(query, db_cursor.connection)
    return df

def fetch_aqi_data(db_cursor):
    query = '''
    SELECT city_aqi_data.city_id, 
           aqi, 
           forecasted_pm25_avg, 
           forecasted_pm10_avg, 
           forecasted_o3_avg, 
           humidity, 
           wind_speed,
           city_weather_data.temperature  -- Adding the temperature column
    FROM city_aqi_data
    JOIN city_weather_data ON city_aqi_data.city_id = city_weather_data.city_id
    '''
    df = pd.read_sql_query(query, db_cursor.connection)
    return df

def save_plot(plt, plot_name):
    plot_folder = os.path.join(os.getcwd(), 'plots') #asked chatGPT how to make a relative path
    if not os.path.exists(plot_folder):
        os.makedirs(plot_folder)
    file_path = os.path.join(plot_folder, f"{plot_name}.png")

    plt.savefig(file_path)
    plt.close()  
    print(f"Plot saved as: {file_path}")

def plot_avg_temperature(df):
    plt.figure(figsize=(12, 6))
    plt.bar(df['city_id'], df['avg_temperature'], color='orange', alpha=0.7)
    plt.title('Average Temperature in Cities')
    plt.xlabel('City ID')
    plt.ylabel('Average Temperature (°C)')
    plt.xticks(rotation=90)
    plt.tight_layout()
    save_plot(plt, 'average_temperature')

def plot_aqi_heatmap(df):
    """Plot a heatmap for AQI and pollutants (PM25, PM10, O3)."""
    heatmap_data = df.pivot_table(index='city_id', 
                                  values=['forecasted_pm25_avg', 'forecasted_pm10_avg', 'forecasted_o3_avg'],
                                  aggfunc='mean')
    plt.figure(figsize=(10, 8))
    sns.heatmap(heatmap_data, annot=True, cmap='coolwarm', fmt=".1f")
    plt.title('Heatmap of AQI across Cities')
    plt.ylabel('City ID')
    plt.xlabel('Pollutants')
    save_plot(plt, 'aqi_heatmap')

def plot_pollutant_comparison(df):
    """Plot a comparison of different pollutants across cities."""
    pollutants = ['forecasted_pm25_avg', 'forecasted_pm10_avg', 'forecasted_o3_avg']
    df_pollutants = df[pollutants].mean().reset_index()
    df_pollutants.columns = ['Pollutant', 'Average Value']

    plt.figure(figsize=(10, 6))
    sns.barplot(x='Pollutant', y='Average Value', data=df_pollutants, palette='Blues')
    plt.title('Average Pollutant Levels in Cities')
    save_plot(plt, 'pollutant_comparison')


def plot_temperature_distribution(df):
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df_clean = df.dropna(subset=['temperature'])
    plt.figure(figsize=(10, 6))
    sns.histplot(df_clean['temperature'], bins=30, kde=True, color='blue', alpha=0.7)
    plt.title('Temperature Distribution Across Cities')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Frequency')
    save_plot(plt, 'temperature_distribution')

def plot_wind_speed_vs_aqi(df):
    df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
    df['aqi'] = pd.to_numeric(df['aqi'], errors='coerce')
    df_clean = df.dropna(subset=['wind_speed', 'aqi'])
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='wind_speed', y='aqi', data=df_clean, color='purple', alpha=0.7)
    plt.title('Wind Speed vs AQI for Cities')
    plt.xlabel('Wind Speed (m/s)')
    plt.ylabel('AQI')
    save_plot(plt, 'wind_speed_vs_aqi')

def plot_correlation_heatmap(df):
    df_corr = df[['aqi', 'forecasted_pm25_avg', 'forecasted_pm10_avg', 'forecasted_o3_avg', 'humidity', 'wind_speed', 'temperature']].corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(df_corr, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
    plt.title('Correlation Heatmap of AQI and Weather Data')
    save_plot(plt, 'correlation_heatmap')

def plot_aqi_boxplot(df):
    plt.figure(figsize=(10, 6))
    sns.boxplot(x='city_id', y='aqi', data=df, color='skyblue')
    plt.title('AQI Distribution Across Cities')
    plt.xlabel('City ID')
    plt.ylabel('AQI')
    plt.xticks(rotation=90)
    save_plot(plt, 'aqi_boxplot')

def main():
    conn = connect_to_db()
    cursor = conn.cursor()
    df_avg_temp = fetch_avg_temperature_data(cursor)
    plot_avg_temperature(df_avg_temp)
    df_aqi = fetch_aqi_data(cursor)
    plot_aqi_heatmap(df_aqi)
    plot_pollutant_comparison(df_aqi)
    plot_temperature_distribution(df_aqi)
    plot_wind_speed_vs_aqi(df_aqi)
    plot_correlation_heatmap(df_aqi)
    plot_aqi_boxplot(df_aqi)

    conn.close()


if __name__ == "__main__":
    main()

