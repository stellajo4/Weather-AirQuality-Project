import requests
from bs4 import BeautifulSoup
import sqlite3

"""Goal of this file: general API/website handling. Functions to get data from any API/site"""

def get_api_data(api_url):
    """
    Fetches data from the API using the provided URL.
    """
    # Send GET request to the API URL
    response = requests.get(api_url)
    if response.status_code == 200:
        # Parse the JSON data
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None