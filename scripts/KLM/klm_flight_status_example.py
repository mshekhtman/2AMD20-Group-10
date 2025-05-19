import os
import requests
import json
from datetime import datetime

def test_flight_status_request():
    """
    Test the flight status API using the example from the documentation:
    
    curl --location 'https://api.airfranceklm.com/opendata/flightstatus?startRange=2025-12-31T09:00:00Z&endRange=2025-12-31T23:59:59Z' \
    --header 'API-Key: [YOUR_API_KEY]' \
    --header 'Content-Type: application/x-www-form-urlencoded'
    """
    # Your API key
    api_key = "qawc94kkpwnkcmch3vgc4cdm"
    
    # Endpoint URL with parameters (exactly as in the example)
    url = "https://api.airfranceklm.com/opendata/flightstatus"
    params = {
        "startRange": "2025-12-31T09:00:00Z",
        "endRange": "2025-12-31T23:59:59Z"
    }
    
    # Headers (exactly as in the example)
    headers = {
        "API-Key": api_key,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    print("Making request to KLM API...")
    print(f"URL: {url}")
    print(f"Params: {params}")
    print(f"Headers: {headers}")
    
    try:
        # Make the request
        response = requests.get(url, headers=headers, params=params)
        
        # Get status code
        status_code = response.status_code
        print(f"\nResponse status code: {status_code}")
        
        # Try to parse as JSON
        try:
            response_json = response.json()
            print("\nResponse (JSON):")
            print(json.dumps(response_json, indent=2))
        except:
            # If not JSON, print as text
            print("\nResponse (Text):")
            print(response.text)
        
        # Save the response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        directory = os.path.join("data", "KLM", "raw")
        os.makedirs(directory, exist_ok=True)  # Ensure directory exists
        
        filename = os.path.join(directory, f"klm_flightstatus_response_{timestamp}.json")
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        
        print(f"\nResponse saved to {filename}")
        
    except Exception as e:
        print(f"\nError making request: {str(e)}")

# Run the test
if __name__ == "__main__":
    test_flight_status_request()
