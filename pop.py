# connection_script.py
import requests
import time
import json
import os
from datetime import datetime

# Directory to save response files
output_dir = "/home/bigdata/Desktop/pop"  # Modify this to match your desired directory
os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist

while True:
    try:
        # Connect to the API to get a response
        response = requests.get("https://datausa.io/api/data?drilldowns=Nation&measures=Population")
        if response.status_code == 200:
            fetched_data = response.json()

            # Create a unique filename based on the current timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(output_dir, f"response_{timestamp}.json")

            # Write the response to a separate file
            with open(filename, 'w') as json_file:
                json.dump(fetched_data, json_file, indent=None)  # Pretty print JSON

            print(f"Data logged to file: {filename}")
        else:
            print("Failed to fetch data from API")

    except Exception as e:
        print(f"An error occurred: {e}")

    time.sleep(1)  # Wait for 10 seconds before the next fetch 

