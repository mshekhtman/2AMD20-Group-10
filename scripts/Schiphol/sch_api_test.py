"""
Schiphol API Test Script
This script tests the connection to the Schiphol API and verifies that it's working.
"""
import requests
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SchipholTest")

def test_schiphol_api():
    """Test the connection to the Schiphol API"""
    logger.info("Testing Schiphol API connection")
   
    # API credentials
    app_id = "df8d5218"
    app_key = "28be4a17746ad28e1646b40fc2060854"
   
    # Base URL - corrected to the actual Schiphol API domain
    base_url = "https://api.schiphol.nl/public"
   
    # Endpoints to test - updated based on API documentation
    endpoints = [
        "/flights",
        "/destinations",
        "/airlines",
        "/aircrafttypes"
    ]
   
    # Test each endpoint
    results = {}
   
    for endpoint in endpoints:
        logger.info(f"Testing endpoint: {endpoint}")
       
        # Set up headers - with correct capitalization and structure
        headers = {
            'Accept': 'application/json',
            'app_id': app_id,
            'app_key': app_key,
            'ResourceVersion': 'v4'
        }
       
        # Construct URL
        url = f"{base_url}{endpoint}"
        
        # For flights endpoint, add required parameters
        params = {}
        if endpoint == "/flights":
            params = {
                'includedelays': 'false',
                'page': '0',
                'sort': '+scheduleTime'
            }
       
        # Make request
        try:
            response = requests.get(url, headers=headers, params=params)
           
            # Check response status
            status_code = response.status_code
            logger.info(f"Response status code: {status_code}")
           
            if status_code == 200:
                logger.info("Endpoint is working!")
               
                # Try to parse as JSON
                try:
                    data = response.json()
                    logger.info(f"Response contains valid JSON")
                   
                    # Check data structure
                    keys = list(data.keys())
                    logger.info(f"JSON keys: {keys}")
                   
                    results[endpoint] = {
                        "status": "success",
                        "status_code": status_code,
                        "keys": keys
                    }
                except json.JSONDecodeError:
                    logger.warning(f"Response is not valid JSON")
                    results[endpoint] = {
                        "status": "invalid_json",
                        "status_code": status_code
                    }
            else:
                logger.error(f"Endpoint failed with status code {status_code}")
                logger.error(f"Response text: {response.text}")
                results[endpoint] = {
                    "status": "error",
                    "status_code": status_code,
                    "message": response.text
                }
        except Exception as e:
            logger.error(f"Error testing endpoint {endpoint}: {str(e)}")
            results[endpoint] = {
                "status": "exception",
                "message": str(e)
            }
   
    # Summarize results
    logger.info("\nTest results summary:")
   
    all_success = True
   
    for endpoint, result in results.items():
        status = result.get("status")
        status_code = result.get("status_code")
       
        if status == "success":
            logger.info(f"✅ {endpoint} - Success ({status_code})")
        else:
            all_success = False
            logger.info(f"❌ {endpoint} - {status} ({status_code})")
   
    if all_success:
        logger.info("\n🎉 All endpoints are working!")
    else:
        logger.warning("\n⚠️ Some endpoints failed. Check the logs for details.")
   
    return results

if __name__ == "__main__":
    test_schiphol_api()