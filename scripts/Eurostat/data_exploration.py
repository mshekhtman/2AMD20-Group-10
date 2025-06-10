import requests, time, logging
import json
from datetime import datetime

class SchipholDataCollector:
    def __init__(self, api_config):
        self.base_url = api_config["https://api.schiphol.nl/public"]
        self.app_id = api_config["df8d5218"]
        self.app_key = api_config["28be4a17746ad28e1646b40fc2060854"]
        self.max_pages = 100  # safety cap for pagination
        logging.info("SchipholDataCollector initialized")

    def fetch_flights(self, start_date, end_date):
        """Fetch all flights in the given date range from Schiphol API."""
        headers = {"ResourceVersion": "v4", "app_id": self.app_id, "app_key": self.app_key}
        params = {"flightdirection": "D", "scheduleDateFrom": start_date, "scheduleDateTo": end_date}
        all_flights = []
        url = self.base_url
        page = 0
        while url and page < self.max_pages:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                logging.error(f"Failed to fetch flights: HTTP {response.status_code}")
                break
            data = response.json()
            flights = data.get("flights", [])
            all_flights.extend(flights)
            logging.info(f"Fetched {len(flights)} flights from page {page}")
            # Check for pagination link (if more pages)
            if 'link' in response.headers:
                # Parse 'link' header for next page URL (Schiphol API provides it if available)
                next_link = self._parse_next_link(response.headers['link'])
            else:
                next_link = None
            url = next_link
            page += 1
            time.sleep(1)  # brief pause to respect any rate limits
            logging.info(f"Total flights fetched: {len(all_flights)}")
            # Save to raw file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            raw_path = f"data/Schiphol/raw/flights_{start_date}_{end_date}_{timestamp}.json"
            with open(raw_path, "w") as f:
                json.dump({"flights": all_flights}, f)
            return raw_path
    
    def _parse_next_link(self, link_header):
            """
            Parse the 'link' header to extract the next page URL.
            Schiphol API uses RFC 5988 Web Linking (e.g., <url>; rel="next").
            """
            import re
            matches = re.findall(r'<([^>]+)>;\s*rel="next"', link_header)
            return matches[0] if matches else None

import requests, time, logging

class SchipholDataCollector:
    def __init__(self, api_config):
        self.base_url = api_config["base_url"]  # e.g., "https://api.schiphol.nl/public-flights/flights"
        self.app_id = api_config["app_id"]
        self.app_key = api_config["app_key"]
        self.max_pages = 100  # safety cap for pagination
        logging.info("SchipholDataCollector initialized")

    def fetch_flights(self, start_date, end_date):
        """Fetch all flights in the given date range from Schiphol API."""
        headers = {"ResourceVersion": "v4", "app_id": self.app_id, "app_key": self.app_key}
        params = {"flightdirection": "D", "scheduleDateFrom": start_date, "scheduleDateTo": end_date}
        all_flights = []
        url = self.base_url
        page = 0
        while url and page < self.max_pages:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                logging.error(f"Failed to fetch flights: HTTP {response.status_code}")
                break
            data = response.json()
            flights = data.get("flights", [])
            all_flights.extend(flights)
            logging.info(f"Fetched {len(flights)} flights from page {page}")
            # Check for pagination link (if more pages)
            if 'link' in response.headers:
                # Parse 'link' header for next page URL (Schiphol API provides it if available)
                next_link = self._parse_next_link(response.headers['link'])
            else:
                next_link = None
            url = next_link
            page += 1
            time.sleep(1)  # brief pause to respect any rate limits
        logging.info(f"Total flights fetched: {len(all_flights)}")
        # Save to raw file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_path = f"data/Schiphol/raw/flights_{start_date}_{end_date}_{timestamp}.json"
        with open(raw_path, "w") as f:
            json.dump({"flights": all_flights}, f)
        return raw_path
    
    
