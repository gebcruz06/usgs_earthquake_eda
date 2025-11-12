import requests
import json
from datetime import date, timedelta

start_date = date.today() - timedelta(7) # 7 days
end_date = date.today() - timedelta(1)

output_dir = f'./data/raw'

# Construct the API URL with start and end dates formatted for geojson output.
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

try:
    # Make the GET request to fetch data
    response = requests.get(url)

    # Check if the request was successful
    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    data = response.json().get('features', [])

    if not data:
        print("No data returned for the specified date range.")
    else:
        file_path = f'{output_dir}/{start_date}_earthquake_data.json'

        # Save the JSON data
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data successfully saved to {file_path}")

except requests.exceptions.RequestException as e:
    print(f"Error fetching data from API: {e}")