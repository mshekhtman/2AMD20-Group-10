"""
Schiphol API Data Collector

This script collects aviation data from the Schiphol API for the KLM hub expansion project.
Uses the confirmed working configuration found through troubleshooting.
"""

import requests
import json
import os
import logging
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("schiphol_data_collection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SchipholCollector")

class SchipholApiCollector:
    """Collector for Schiphol API data"""
    
    def __init__(self, app_id="df8d5218", app_key="28be4a17746ad28e1646b40fc2060854", output_dir='data/Schiphol/raw'):
        """Initialize the Schiphol API collector with the working configuration"""
        # Set up API credentials
        self.app_id = app_id
        self.app_key = app_key
        
        # Working base URL and endpoints (confirmed through testing)
        self.base_url = "https://api.schiphol.nl"
        self.flights_endpoint = "/public-flights/flights"
        self.airlines_endpoint = "/public-flights/airlines"
        self.destinations_endpoint = "/public-flights/destinations"
        self.aircraft_types_endpoint = "/public-flights/aircrafttypes"
        
        # Set up output directory
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set up rate limiting
        self.calls_per_minute = 30  # Conservative value
        self.min_interval = 60.0 / self.calls_per_minute  # Seconds between calls
        self.last_call_time = None
        
        logger.info("Schiphol API Collector initialized with working configuration")
    
    def _respect_rate_limit(self):
        """Enforce rate limiting to avoid hitting API limits"""
        current_time = time.time()
        
        # Check per-minute limit
        if self.last_call_time is not None:
            elapsed = current_time - self.last_call_time
            if elapsed < self.min_interval:
                wait_time = self.min_interval - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
        
        self.last_call_time = time.time()
    
    def make_request(self, endpoint, params=None):
        """Make an authenticated request to the Schiphol API using the working configuration"""
        # Respect rate limits
        self._respect_rate_limit()
        
        # Construct URL
        url = f"{self.base_url}{endpoint}"
        
        # Set up headers with API credentials
        headers = {
            'Accept': 'application/json',
            'ResourceVersion': 'v4',
            'app_id': self.app_id,
            'app_key': self.app_key
        }
        
        logger.info(f"Making request to {endpoint}")
        
        # Make request
        try:
            response = requests.get(url, headers=headers, params=params)
            
            # Check for pagination headers
            if 'link' in response.headers:
                logger.info(f"Pagination header found: {response.headers['link']}")
            
            # Check response status
            if response.status_code == 200:
                # Try to parse as JSON
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logger.warning(f"Response is not valid JSON: {response.text[:100]}...")
                    return {"error": "Not valid JSON", "text": response.text}
            else:
                logger.error(f"Request failed with status code {response.status_code}: {response.text[:100]}...")
                return {"error": f"Status code {response.status_code}", "text": response.text}
                
        except Exception as e:
            logger.error(f"Error making request: {str(e)}")
            return {"error": str(e)}
    
    def save_data(self, data, filename):
        """Save collected data to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.output_dir, f"{filename}_{timestamp}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved data to {filepath}")
        return filepath
    
    def collect_flights(self, flight_direction=None, page=0, page_size=20, include_delays=True):
        """
        Collect flight data from Schiphol API
        
        Args:
            flight_direction: Direction of flights to collect ('arrival' or 'departure', None for all)
            page: Page number for pagination
            page_size: Number of results per page
            include_delays: Whether to include delay information
        """
        direction_text = flight_direction if flight_direction else "all"
        logger.info(f"Collecting {direction_text} flights (page {page})")
        
        # Set up parameters
        params = {
            'page': page,
            'sort': '+scheduleTime',
            'includedelays': str(include_delays).lower()
        }
        
        # Add flight direction if specified
        if flight_direction in ['arrival', 'departure']:
            params['flightDirection'] = flight_direction
        
        # Make request
        data = self.make_request(self.flights_endpoint, params)
        
        # Save data
        if data and "error" not in data:
            self.save_data(data, f"flights_{direction_text}_page{page}")
        
        return data
    
    def collect_all_flights(self, flight_direction=None, max_pages=5, include_delays=True):
        """
        Collect multiple pages of flight data
        
        Args:
            flight_direction: Direction of flights to collect ('arrival' or 'departure', None for all)
            max_pages: Maximum number of pages to collect
            include_delays: Whether to include delay information
        """
        direction_text = flight_direction if flight_direction else "all"
        logger.info(f"Collecting {direction_text} flights (max {max_pages} pages)")
        
        all_flights = []
        page = 0
        
        while page < max_pages:
            # Collect one page of flights
            data = self.collect_flights(flight_direction, page, include_delays=include_delays)
            
            # Check for errors
            if not data or "error" in data:
                logger.warning(f"Error collecting page {page}. Stopping pagination.")
                break
            
            # Get flights array
            flights = data.get('flights', [])
            all_flights.extend(flights)
            
            logger.info(f"Collected {len(flights)} flights from page {page}")
            
            # Check if we've reached the end
            if len(flights) < 20:  # Default page size is 20
                logger.info(f"Reached end of results at page {page}")
                break
            
            # Move to next page
            page += 1
            
            # Add a small delay between requests
            time.sleep(1)
        
        # Save all flights to a single file
        if all_flights:
            combined_data = {'flights': all_flights}
            self.save_data(combined_data, f"all_flights_{direction_text}")
            logger.info(f"Collected {len(all_flights)} total {direction_text} flights")
        
        return all_flights
    
    def collect_destinations(self):
        """Collect destination data from Schiphol API"""
        logger.info("Collecting destinations")
        
        # Make request
        data = self.make_request(self.destinations_endpoint)
        
        # Save data
        if data and "error" not in data:
            self.save_data(data, "destinations")
        
        return data
    
    def collect_airlines(self):
        """Collect airline data from Schiphol API"""
        logger.info("Collecting airlines")
        
        # Make request
        data = self.make_request(self.airlines_endpoint)
        
        # Save data
        if data and "error" not in data:
            self.save_data(data, "airlines")
        
        return data
    
    def collect_aircraft_types(self):
        """Collect aircraft type data from Schiphol API"""
        logger.info("Collecting aircraft types")
        
        # Make request
        data = self.make_request(self.aircraft_types_endpoint)
        
        # Save data
        if data and "error" not in data:
            self.save_data(data, "aircraft_types")
        
        return data
    
    def collect_all(self):
        """Collect all available data from Schiphol API"""
        logger.info("Starting complete Schiphol API data collection")
        
        results = {}
        
        # Collect all flights (first 5 pages)
        results['all_flights'] = self.collect_all_flights(max_pages=5)
        
        # Collect departures (first 5 pages)
        results['departures'] = self.collect_all_flights('departure', max_pages=5)
        
        # Collect arrivals (first 5 pages)
        results['arrivals'] = self.collect_all_flights('arrival', max_pages=5)
        
        # Collect destinations
        results['destinations'] = self.collect_destinations()
        
        # Collect airlines
        results['airlines'] = self.collect_airlines()
        
        # Collect aircraft types
        results['aircraft_types'] = self.collect_aircraft_types()
        
        logger.info("Complete Schiphol API data collection finished")
        return results

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Collect data from Schiphol API")
    parser.add_argument("--endpoint", choices=["all", "flights", "departures", "arrivals", "destinations", "airlines", "aircraft_types"], 
                      default="all", help="Specific endpoint to collect")
    
    args = parser.parse_args()
    
    # Create collector with embedded credentials
    collector = SchipholApiCollector()
    
    # Collect data based on specified endpoint
    if args.endpoint == "all":
        collector.collect_all()
    elif args.endpoint == "flights":
        collector.collect_all_flights(max_pages=5)
    elif args.endpoint == "departures":
        collector.collect_all_flights('departure', max_pages=5)
    elif args.endpoint == "arrivals":
        collector.collect_all_flights('arrival', max_pages=5)
    elif args.endpoint == "destinations":
        collector.collect_destinations()
    elif args.endpoint == "airlines":
        collector.collect_airlines()
    elif args.endpoint == "aircraft_types":
        collector.collect_aircraft_types()

if __name__ == "__main__":
    main()