"""
KLM Flight Data Processor

This script processes the flight status data from the KLM API into structured formats.
"""

import json
import os
import pandas as pd
import logging
import glob
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("flight_data_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FlightProcessor")

class FlightDataProcessor:
    """Processor for KLM flight status data"""
    
    def __init__(self, raw_dir='data/raw/klm_api', processed_dir='data/processed', specific_file=None):
        """
        Initialize the flight data processor
        
        Args:
            raw_dir: Directory containing raw KLM API data
            processed_dir: Directory for processed data
            specific_file: Optional specific file to process instead of searching in raw_dir
        """
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.specific_file = specific_file
        os.makedirs(processed_dir, exist_ok=True)
        
        logger.info("Flight Data Processor initialized")
    
    def find_flight_status_file(self):
        """Find the latest flight status file"""
        # First check if a specific file was provided
        if self.specific_file and os.path.exists(self.specific_file):
            logger.info(f"Using specific file: {self.specific_file}")
            return self.specific_file
        
        # Check for files in the project root
        root_pattern = "klm_flightstatus_response_*.json"
        root_files = glob.glob(root_pattern)
        if root_files:
            latest_file = max(root_files, key=os.path.getmtime)
            logger.info(f"Found flight status file in project root: {latest_file}")
            return latest_file
        
        # Look in the raw directory
        pattern = os.path.join(self.raw_dir, "flight_status_*.json")
        files = glob.glob(pattern)
        
        if not files:
            logger.error("No flight status files found")
            return None
        
        # Sort by modification time (newest first)
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found flight status file in raw directory: {latest_file}")
        
        return latest_file
    
    def process_flights(self):
        """Process flight status data into structured formats"""
        logger.info("Processing flight status data")
        
        # Find the flight status file
        filepath = self.find_flight_status_file()
        if not filepath:
            return None
        
        # Load the data
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Successfully loaded data from {filepath}")
            
            # Check for operationalFlights key (in this specific format)
            if "operationalFlights" not in data:
                logger.error("Data does not contain 'operationalFlights' key. Raw keys found: " + str(list(data.keys())))
                return None
                
            # Use operationalFlights as the flight data
            flights_data = data["operationalFlights"]
            logger.info(f"Found {len(flights_data)} operational flights in the data")
                
        except Exception as e:
            logger.error(f"Error loading flight status data: {str(e)}")
            return None
        
        # Extract flight information
        flights = []
        
        for flight in flights_data:
            # Basic flight information
            flight_info = {
                "flight_number": flight.get("flightNumber"),
                "flight_date": flight.get("flightScheduleDate"),
                "flight_id": flight.get("id"),
                "status": flight.get("flightStatusPublic")
            }
            
            # Add airline information
            if "airline" in flight:
                flight_info["airline_code"] = flight["airline"].get("code")
                flight_info["airline_name"] = flight["airline"].get("name")
            
            # Route information
            if "route" in flight and flight["route"]:
                flight_info["route"] = "->".join(flight["route"])
                flight_info["origin"] = flight["route"][0] if len(flight["route"]) > 0 else None
                flight_info["destination"] = flight["route"][-1] if len(flight["route"]) > 0 else None
            
            # Process flight legs
            if "flightLegs" in flight:
                leg_count = len(flight["flightLegs"])
                logger.info(f"Flight {flight_info['flight_number']} has {leg_count} legs")
                
                for leg in flight["flightLegs"]:
                    leg_info = flight_info.copy()
                    
                    # Leg status
                    leg_info["leg_status"] = leg.get("legStatusPublic")
                    leg_info["leg_status_name"] = leg.get("legStatusPublicLangTransl")
                    
                    # Departure information
                    dep_info = leg.get("departureInformation", {})
                    if "airport" in dep_info:
                        airport = dep_info["airport"]
                        leg_info["departure_airport_code"] = airport.get("code")
                        leg_info["departure_airport_name"] = airport.get("name")
                        
                        # City information
                        if "city" in airport:
                            city = airport["city"]
                            leg_info["departure_city_code"] = city.get("code")
                            leg_info["departure_city_name"] = city.get("name")
                            
                            # Country information
                            if "country" in city:
                                country = city["country"]
                                leg_info["departure_country_code"] = country.get("code")
                                leg_info["departure_country_name"] = country.get("name")
                        
                        # Location information
                        if "location" in airport:
                            leg_info["departure_latitude"] = airport["location"].get("latitude")
                            leg_info["departure_longitude"] = airport["location"].get("longitude")
                    
                    # Departure time information
                    if "times" in dep_info:
                        leg_info["scheduled_departure_time"] = dep_info["times"].get("scheduled")
                        leg_info["latest_departure_time"] = dep_info["times"].get("latestPublished")
                    
                    # Arrival information
                    arr_info = leg.get("arrivalInformation", {})
                    if "airport" in arr_info:
                        airport = arr_info["airport"]
                        leg_info["arrival_airport_code"] = airport.get("code")
                        leg_info["arrival_airport_name"] = airport.get("name")
                        
                        # City information
                        if "city" in airport:
                            city = airport["city"]
                            leg_info["arrival_city_code"] = city.get("code")
                            leg_info["arrival_city_name"] = city.get("name")
                            
                            # Country information
                            if "country" in city:
                                country = city["country"]
                                leg_info["arrival_country_code"] = country.get("code")
                                leg_info["arrival_country_name"] = country.get("name")
                        
                        # Location information
                        if "location" in airport:
                            leg_info["arrival_latitude"] = airport["location"].get("latitude")
                            leg_info["arrival_longitude"] = airport["location"].get("longitude")
                    
                    # Arrival time information
                    if "times" in arr_info:
                        leg_info["scheduled_arrival_time"] = arr_info["times"].get("scheduled")
                        leg_info["latest_arrival_time"] = arr_info["times"].get("latestPublished")
                        
                        # Estimated arrival time
                        if "estimated" in arr_info["times"]:
                            leg_info["estimated_arrival_time"] = arr_info["times"]["estimated"].get("value")
                    
                    # Flight details
                    leg_info["scheduled_duration"] = leg.get("scheduledFlightDuration")
                    leg_info["completion_percentage"] = leg.get("completionPercentage")
                    
                    # Aircraft information
                    if "aircraft" in leg:
                        aircraft = leg["aircraft"]
                        leg_info["aircraft_type_code"] = aircraft.get("typeCode")
                        leg_info["aircraft_type_name"] = aircraft.get("typeName")
                        leg_info["aircraft_owner"] = aircraft.get("ownerAirlineName")
                    
                    flights.append(leg_info)
                    
            else:
                # If no flight legs, just add the basic flight info
                flights.append(flight_info)
        
        # Convert to DataFrame
        if not flights:
            logger.warning("No flight data extracted")
            return None
        
        df = pd.DataFrame(flights)
        
        # Save as CSV
        flights_csv = os.path.join(self.processed_dir, "flights.csv")
        df.to_csv(flights_csv, index=False)
        
        logger.info(f"Saved {len(flights)} flights to {flights_csv}")
        
        # Extract and process airports
        self.process_airports(flights)
        
        # Extract and process airlines
        self.process_airlines(flights)
        
        # Extract and process routes
        self.process_routes(flights)
        
        return df
    
    def process_airports(self, flights):
        """Extract and process airport information"""
        logger.info("Extracting airport information")
        
        airports = []
        
        for flight in flights:
            # Process departure airport
            if "departure_airport_code" in flight and flight["departure_airport_code"]:
                airport = {
                    "airport_code": flight["departure_airport_code"],
                    "airport_name": flight.get("departure_airport_name", ""),
                    "city_code": flight.get("departure_city_code", ""),
                    "city_name": flight.get("departure_city_name", ""),
                    "country_code": flight.get("departure_country_code", ""),
                    "country_name": flight.get("departure_country_name", ""),
                    "latitude": flight.get("departure_latitude"),
                    "longitude": flight.get("departure_longitude")
                }
                airports.append(airport)
            
            # Process arrival airport
            if "arrival_airport_code" in flight and flight["arrival_airport_code"]:
                airport = {
                    "airport_code": flight["arrival_airport_code"],
                    "airport_name": flight.get("arrival_airport_name", ""),
                    "city_code": flight.get("arrival_city_code", ""),
                    "city_name": flight.get("arrival_city_name", ""),
                    "country_code": flight.get("arrival_country_code", ""),
                    "country_name": flight.get("arrival_country_name", ""),
                    "latitude": flight.get("arrival_latitude"),
                    "longitude": flight.get("arrival_longitude")
                }
                airports.append(airport)
        
        # Remove duplicates by airport code
        unique_airports = {}
        for airport in airports:
            code = airport["airport_code"]
            if code and code not in unique_airports:
                unique_airports[code] = airport
        
        # Convert to DataFrame
        airport_data = list(unique_airports.values())
        
        if not airport_data:
            logger.warning("No airport data extracted")
            return None
        
        airport_df = pd.DataFrame(airport_data)
        
        # Save as CSV
        airport_csv = os.path.join(self.processed_dir, "airports.csv")
        airport_df.to_csv(airport_csv, index=False)
        
        logger.info(f"Saved {len(airport_data)} airports to {airport_csv}")
        
        return airport_df
    
    def process_airlines(self, flights):
        """Extract and process airline information"""
        logger.info("Extracting airline information")
        
        airlines = []
        
        for flight in flights:
            if "airline_code" in flight and flight["airline_code"]:
                airline = {
                    "airline_code": flight["airline_code"],
                    "airline_name": flight.get("airline_name", "")
                }
                airlines.append(airline)
        
        # Remove duplicates by airline code
        unique_airlines = {}
        for airline in airlines:
            code = airline["airline_code"]
            if code and code not in unique_airlines:
                unique_airlines[code] = airline
        
        # Convert to DataFrame
        airline_data = list(unique_airlines.values())
        
        if not airline_data:
            logger.warning("No airline data extracted")
            return None
        
        airline_df = pd.DataFrame(airline_data)
        
        # Save as CSV
        airline_csv = os.path.join(self.processed_dir, "airlines.csv")
        airline_df.to_csv(airline_csv, index=False)
        
        logger.info(f"Saved {len(airline_data)} airlines to {airline_csv}")
        
        return airline_df
    
    def process_routes(self, flights):
        """Extract and process route information"""
        logger.info("Extracting route information")
        
        routes = []
        
        for flight in flights:
            if "departure_airport_code" in flight and "arrival_airport_code" in flight:
                origin = flight["departure_airport_code"]
                destination = flight["arrival_airport_code"]
                
                if origin and destination:
                    route = {
                        "origin": origin,
                        "destination": destination,
                        "airline_code": flight.get("airline_code", ""),
                        "scheduled_duration": flight.get("scheduled_duration", "")
                    }
                    routes.append(route)
        
        # Remove duplicates by origin-destination pair
        unique_routes = {}
        for route in routes:
            key = f"{route['origin']}-{route['destination']}"
            if key not in unique_routes:
                unique_routes[key] = route
        
        # Convert to DataFrame
        route_data = list(unique_routes.values())
        
        if not route_data:
            logger.warning("No route data extracted")
            return None
        
        route_df = pd.DataFrame(route_data)
        
        # Save as CSV
        route_csv = os.path.join(self.processed_dir, "routes.csv")
        route_df.to_csv(route_csv, index=False)
        
        logger.info(f"Saved {len(route_data)} routes to {route_csv}")
        
        return route_df

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process KLM flight status data")
    parser.add_argument("--file", help="Specific flight status file to process")
    
    args = parser.parse_args()
    
    # Create processor with specific file if provided
    processor = FlightDataProcessor(specific_file=args.file)
    
    # Process flight data
    processor.process_flights()

if __name__ == "__main__":
    main()