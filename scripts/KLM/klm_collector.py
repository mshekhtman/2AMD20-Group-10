"""
KLM API Data Collector

This script collects data from the KLM API for the multi-hub expansion knowledge graph project.
Includes proper rate limiting and error handling.
"""

import requests
import json
import os
import logging
import time
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("klm_data_collection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KLMCollector")

class KLMApiCollector:
    """Collector for KLM API data with rate limiting"""
    
    def __init__(self, api_key, output_dir='data/KLM/raw'):
        """Initialize the KLM API collector"""
        self.api_key = api_key
        self.base_url = "https://api.airfranceklm.com"
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Track API calls for rate limiting
        self.last_call_time = None
        self.min_call_interval = 1.0  # 1 second between calls
        
        logger.info("KLM API Collector initialized")
    
    def _respect_rate_limit(self):
        """Enforce rate limiting to avoid 'Developer Over QPS' errors"""
        current_time = time.time()
        
        if self.last_call_time is not None:
            # Calculate time since last API call
            elapsed = current_time - self.last_call_time
            
            # If it's been less than our minimum interval, wait
            if elapsed < self.min_call_interval:
                wait_time = self.min_call_interval - elapsed
                logger.info(f"Rate limiting: waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
        
        # Update last call time
        self.last_call_time = time.time()
    
    def make_request(self, endpoint, params=None):
        """Make a request to the KLM API using the working format"""
        # Respect rate limits
        self._respect_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        # Headers - using the format that worked in your test
        headers = {
            'API-Key': self.api_key,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        logger.info(f"Making request to {endpoint}")
        
        # Make the request
        try:
            response = requests.get(url, headers=headers, params=params)
            
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
    
    def collect_flight_status(self, start_date=None, end_date=None):
        """Collect flight status data"""
        logger.info("Collecting flight status data")
        
        # Use provided dates or default to current date
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%dT00:00:00Z")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%dT23:59:59Z")
        
        # Set parameters for the request
        params = {
            "startRange": start_date,
            "endRange": end_date
        }
        
        # Make the request
        data = self.make_request("/opendata/flightstatus", params)
        
        # Save the data
        return self.save_data(data, "flight_status")
    
    def test_all_endpoints(self):
        """Test all possible endpoints to determine which ones work"""
        logger.info("Testing all potential KLM API endpoints")
        
        endpoints = [
            "/opendata/flightstatus",
            "/opendata/network-and-schedule",
            "/opendata/inspire/amenities",
            "/opendata/baggages",
            "/opendata/flightoffers",
            "/opendata/flightstatus/v4/flights"
        ]
        
        results = {}
        working_endpoints = []
        
        for endpoint in endpoints:
            logger.info(f"Testing endpoint: {endpoint}")
            
            # Make a test request
            result = self.make_request(endpoint)
            
            # Save result
            results[endpoint] = {
                "status": "success" if "error" not in result else "error",
                "error": result.get("error", None)
            }
            
            # Check if it worked
            if "error" not in result:
                working_endpoints.append(endpoint)
                logger.info(f"Endpoint {endpoint} is working!")
            else:
                logger.warning(f"Endpoint {endpoint} failed with error: {result.get('error')}")
            
            # Add a longer delay between tests to avoid rate limiting
            time.sleep(2)
        
        # Save test results
        self.save_data(results, "endpoint_test_results")
        self.save_data({"working_endpoints": working_endpoints}, "working_endpoints")
        
        logger.info(f"Endpoint testing complete. Working endpoints: {working_endpoints}")
        return working_endpoints

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Collect data from KLM API")
    parser.add_argument("--api-key", default="qawc94kkpwnkcmch3vgc4cdm", help="KLM API key")
    parser.add_argument("--action", choices=["test", "flight_status"], 
                        default="test", help="Action to perform")
    
    args = parser.parse_args()
    
    # Create collector
    collector = KLMApiCollector(args.api_key)
    
    # Perform action
    if args.action == "test":
        collector.test_all_endpoints()
    elif args.action == "flight_status":
        collector.collect_flight_status()

if __name__ == "__main__":
    main()