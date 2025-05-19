#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import sys
import json
import time

def test_endpoint(url, headers, params=None, description=""):
    """Test a specific endpoint with given headers and parameters"""
    print(f"\n--- Testing: {description} ---")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    if params:
        print(f"Parameters: {params}")
    
    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS!")
            data = response.json()
            print(f"Response data keys: {list(data.keys())}")
            return True, data
        else:
            print(f"❌ ERROR: {response.text}")
            print("\nResponse Headers:")
            for key, value in response.headers.items():
                print(f"{key}: {value}")
            return False, None
            
    except Exception as e:
        print(f"❌ EXCEPTION: {str(e)}")
        return False, None

def test_schiphol_api():
    """Test multiple variations of Schiphol API endpoints"""
    print("==== Schiphol API Troubleshooting ====")

    # API credentials
    app_id = "df8d5218"
    app_key = "28be4a17746ad28e1646b40fc2060854"

    # Base URLs to try
    base_urls = [
        "https://api.schiphol.nl/public",
        "https://api.schiphol.nl",
        "https://rest.developer.schiphol.nl",
    ]
    
    # Endpoints to try
    endpoints = [
        "/flights",
        "/public-flights/flights",
        "/flights/departure",
        "/flights/arrival",
        "/destinations",
        "/airlines"
    ]
    
    # API versions to try
    versions = ["v4", "v3", "v2"]
    
    # Standard headers
    base_headers = {
        'Accept': 'application/json',
    }
    
    # Try different header combinations
    header_styles = [
        # v4 style: app_id and app_key in headers
        {**base_headers, 'ResourceVersion': 'v4', 'app_id': app_id, 'app_key': app_key},
        
        # v3 style: app_id and app_key in headers, different caps
        {**base_headers, 'ResourceVersion': 'v3', 'App-Id': app_id, 'App-Key': app_key},
        
        # Another style with headers capitalized
        {**base_headers, 'ResourceVersion': 'v4', 'App-Id': app_id, 'App-Key': app_key}
    ]
    
    # Standard parameters
    params = {
        'includedelays': 'false',
        'page': 0,
        'sort': '+scheduleTime'
    }
    
    # Set to True if we find any working combination
    success_found = False
    
    # Try different combinations
    for base_url in base_urls:
        for endpoint in endpoints:
            for headers in header_styles:
                # Skip after first success to avoid too many API calls
                if success_found:
                    break
                    
                full_url = f"{base_url}{endpoint}"
                version = headers.get('ResourceVersion', 'unknown')
                description = f"API v{version} - {full_url}"
                
                success, data = test_endpoint(full_url, headers, params, description)
                
                if success:
                    success_found = True
                    print("\n✨ WORKING CONFIGURATION FOUND!")
                    print(f"Base URL: {base_url}")
                    print(f"Endpoint: {endpoint}")
                    print(f"Headers: {headers}")
                    print(f"Parameters: {params}")
                    
                    # Save successful response to file
                    with open('schiphol_success.json', 'w') as f:
                        json.dump(data, f, indent=2)
                        print(f"Full response saved to schiphol_success.json")
                    
                    # No need to try more combinations
                    break
                
                # Add a delay to avoid hitting rate limits
                time.sleep(1)
    
    if not success_found:
        print("\n❗ No working API configuration found.")
        print("Possible issues:")
        print("1. The API credentials might not be active or valid")
        print("2. The API service might have changed its endpoint structure")
        print("3. There might be IP restrictions or other access limitations")
        print("4. The API might be temporarily unavailable")
        print("\nSuggested actions:")
        print("1. Verify your credentials in the Schiphol developer portal")
        print("2. Check if there are any announcements about API changes or maintenance")
        print("3. Contact Schiphol API support for assistance")

if __name__ == '__main__':
    test_schiphol_api()