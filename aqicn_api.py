from api_functions import get_api_data

key = '2f4c73a57b156c067a7fb45fa17784bd26cd0a8f'
request_made = "https://api.waqi.info/feed/shanghai/?token=2f4c73a57b156c067a7fb45fa17784bd26cd0a8f"

data = get_api_data(request_made)
print(data)

#fetch_air_quality()