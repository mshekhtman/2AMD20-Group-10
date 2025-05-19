"""
KLM API Data Collector

This script collects data from all available KLM API endpoints using both direct API key 
and bearer token authentication methods.
"""

import requests
import json
import os
import logging
import time
import base64
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
    """Collector for KLM API data with support for all available endpoints"""
    
    def __init__(self, output_dir='data/KLM/raw'):
        """
        Initialize the KLM API collector with hardcoded credentials
        
        Args:
            output_dir: Directory to save raw API responses
        """
        # Hardcoded credentials
        self.api_key = "qawc94kkpwnkcmch3vgc4cdm"
        self.api_secret = "Pb72TS5I6l"
        
        self.base_url = "https://api.airfranceklm.com"
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Track API calls for rate limiting
        self.last_call_time = None
        self.min_call_interval = 1.0  # 1 second between calls
        
        # Bearer token cache
        self.access_token = None
        self.token_expiry = None
        
        logger.info("KLM API Collector initialized with hardcoded credentials")
    
    def _respect_rate_limit(self):
        """Enforce rate limiting to avoid API throttling"""
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
    
    def get_bearer_token(self):
        """
        Get a bearer token using client credentials flow
        Returns access token or None if unsuccessful
        """
        # Check if we have a valid token already
        current_time = datetime.now()
        if self.access_token and self.token_expiry and current_time < self.token_expiry:
            logger.info("Using existing bearer token")
            return self.access_token
            
        logger.info("Requesting new bearer token")
        
        # Create the authorization header with base64 encoding
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        # Endpoint for token request
        url = f"{self.base_url}/cid/token?client_id={self.api_key}"
        
        # Headers and body for token request
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }
        
        body = {
            "grant_type": "client_credentials"
        }
        
        try:
            response = requests.post(url, headers=headers, json=body)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get("access_token")
                expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
                
                # Set expiry time with a small buffer
                self.token_expiry = current_time + timedelta(seconds=expires_in - 60)
                
                logger.info(f"Bearer token obtained, expires in {expires_in} seconds")
                return self.access_token
            else:
                logger.error(f"Failed to get bearer token: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting bearer token: {str(e)}")
            return None
    
    def make_request(self, endpoint, params=None, use_bearer_token=False):
        """
        Make a request to the KLM API
        
        Args:
            endpoint: API endpoint to call
            params: Query parameters
            use_bearer_token: Whether to use bearer token authentication
        """
        # Respect rate limits
        self._respect_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        # Set up headers based on authentication method
        if use_bearer_token:
            # Get bearer token
            token = self.get_bearer_token()
            if not token:
                logger.error("Cannot make bearer token request without valid token")
                return {"error": "No valid bearer token"}
                
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
        else:
            # Use direct API key authentication
            headers = {
                "API-Key": self.api_key,
                "Content-Type": "application/x-www-form-urlencoded"
            }
        
        logger.info(f"Making request to {endpoint} (Auth: {'Bearer Token' if use_bearer_token else 'API Key'})")
        
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
    
    def save_data(self, data, filename_prefix):
        """Save collected data to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.output_dir, f"{filename_prefix}_{timestamp}.json")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            if isinstance(data, dict) or isinstance(data, list):
                json.dump(data, f, indent=2)
            else:
                f.write(str(data))
        
        logger.info(f"Saved data to {filepath}")
        return filepath
    
    def collect_flight_status(self, start_date=None, end_date=None):
        """
        Collect flight status data using the opendata/flightstatus endpoint
        
        Args:
            start_date: Start date in ISO format (e.g., "2025-05-19T00:00:00Z")
            end_date: End date in ISO format (e.g., "2025-05-19T23:59:59Z")
        """
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
        
        # Make the request using direct API key authentication
        data = self.make_request("/opendata/flightstatus", params)
        
        # Save the data
        if "error" not in data:
            return self.save_data(data, "klm_flightstatus_response")
        else:
            logger.error(f"Failed to collect flight status data: {data.get('error')}")
            return None
    
    def collect_baggage_allowance(self):
        """Collect baggage allowance data"""
        logger.info("Collecting baggage allowance data")
        
        # Make the request using direct API key authentication
        data = self.make_request("/opendata/baggages")
        
        # Save the data
        if "error" not in data:
            return self.save_data(data, "klm_baggage_response")
        else:
            logger.error(f"Failed to collect baggage data: {data.get('error')}")
            return None
    
    def collect_inspire_data(self):
        """Collect inspire/amenities data"""
        logger.info("Collecting inspire/amenities data")
        
        # Make the request using direct API key authentication
        data = self.make_request("/opendata/inspire/amenities")
        
        # Save the data
        if "error" not in data:
            return self.save_data(data, "klm_inspire_response")
        else:
            logger.error(f"Failed to collect inspire data: {data.get('error')}")
            return None
    
    def collect_offers_data(self):
        """Collect flight offers data"""
        logger.info("Collecting flight offers data")
        
        # Make the request using direct API key authentication
        data = self.make_request("/opendata/flightoffers")
        
        # Save the data
        if "error" not in data:
            return self.save_data(data, "klm_offers_response")
        else:
            logger.error(f"Failed to collect offers data: {data.get('error')}")
            return None
    
    def collect_all_data(self):
        """Collect data from all available endpoints"""
        logger.info("Collecting data from all available KLM API endpoints")
        
        results = {}
        
        # Flight Status API
        results["flight_status"] = self.collect_flight_status()
        
        # Baggage Allowance API
        results["baggage"] = self.collect_baggage_allowance()
        
        # Inspire API
        results["inspire"] = self.collect_inspire_data()
        
        # Offers API
        results["offers"] = self.collect_offers_data()
        
        logger.info("Completed collecting data from all KLM API endpoints")
        
        # Save summary
        self.save_data({
            "collection_time": datetime.now().isoformat(),
            "results": {k: "Success" if v else "Failed" for k, v in results.items()}
        }, "klm_collection_summary")
        
        return results

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Collect data from KLM API")
    parser.add_argument("--endpoint", choices=["all", "flight_status", "baggage", "inspire", "offers"], 
                      default="all", help="Specific endpoint to collect data from")
    
    args = parser.parse_args()
    
    # Create collector with hardcoded credentials
    collector = KLMApiCollector()
    
    # Collect data based on specified endpoint
    if args.endpoint == "all":
        collector.collect_all_data()
    elif args.endpoint == "flight_status":
        collector.collect_flight_status()
    elif args.endpoint == "baggage":
        collector.collect_baggage_allowance()
    elif args.endpoint == "inspire":
        collector.collect_inspire_data()
    elif args.endpoint == "offers":
        collector.collect_offers_data()

if __name__ == "__main__":
    main()