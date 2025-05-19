"""
KLM Flight Data Processor

This script processes data from all available KLM API endpoints into structured formats
for the knowledge graph.
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
        logging.FileHandler("klm_data_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KLMProcessor")

class KLMDataProcessor:
    """Processor for KLM API data"""
    
    def __init__(self, raw_dir='data/KLM/raw', processed_dir='data/KLM/processed'):
        """
        Initialize the KLM data processor
        
        Args:
            raw_dir: Directory containing raw KLM API data
            processed_dir: Directory for processed data
        """
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        os.makedirs(processed_dir, exist_ok=True)
        
        logger.info("KLM Data Processor initialized")
    
    def get_latest_file(self, prefix):
        """Get the latest file with the given prefix"""
        pattern = os.path.join(self.raw_dir, f"{prefix}_*.json")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No files found with prefix {prefix}")
            return None
        
        # Sort by modification time (most recent first)
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Latest file for {prefix}: {latest_file}")
        
        return latest_file
    
    def load_json_data(self, filepath):
        """Load JSON data from a file"""
        if not filepath:
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Loaded data from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading data from {filepath}: {str(e)}")
            return None
    
    def process_flight_status(self):
        """Process flight status data"""
        logger.info("Processing flight status data")
        
        # Get the latest flight status file
        filepath = self.get_latest_file("klm_flightstatus_response")
        if not filepath:
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data:
            return None
        
        # Check if there are flights in the data
        if "operationalFlights" not in data:
            logger.warning("No operational flights found in the data")
            return None
        
        # Extract flight information
        flights = []
        
        for flight in data["operationalFlights"]:
            flight_info = {
                "flight_number": flight.get("flightNumber"),
                "flight_date": flight.get("flightScheduleDate"),
                "flight_id": flight.get("id"),
                "airline_code": flight.get("airline", {}).get("code"),
                "airline_name": flight.get("airline", {}).get("name"),
                "status": flight.get("flightStatusPublic")
            }
            
            # Extract route information
            if "route" in flight:
                flight_info["origin"] = flight["route"][0] if len(flight["route"]) > 0 else None
                flight_info["destination"] = flight["route"][-1] if len(flight["route"]) > 0 else None
            
            # Extract leg information
            if "flightLegs" in flight and flight["flightLegs"]:
                for leg in flight["flightLegs"]:
                    leg_info = flight_info.copy()  # Start with the flight information
                    
                    # Add leg-specific information
                    leg_info["leg_status"] = leg.get("legStatusPublic")
                    
                    # Add departure information
                    dep_info = leg.get("departureInformation", {})
                    if "airport" in dep_info:
                        leg_info["departure_airport_code"] = dep_info["airport"].get("code")
                        leg_info["departure_airport_name"] = dep_info["airport"].get("name")
                        
                        if "city" in dep_info["airport"]:
                            leg_info["departure_city"] = dep_info["airport"]["city"].get("name")
                            
                            if "country" in dep_info["airport"]["city"]:
                                leg_info["departure_country"] = dep_info["airport"]["city"]["country"].get("name")
                        
                        if "location" in dep_info["airport"]:
                            leg_info["departure_latitude"] = dep_info["airport"]["location"].get("latitude")
                            leg_info["departure_longitude"] = dep_info["airport"]["location"].get("longitude")
                    
                    # Add departure time
                    if "times" in dep_info:
                        leg_info["scheduled_departure_time"] = dep_info["times"].get("scheduled")
                    
                    # Add arrival information
                    arr_info = leg.get("arrivalInformation", {})
                    if "airport" in arr_info:
                        leg_info["arrival_airport_code"] = arr_info["airport"].get("code")
                        leg_info["arrival_airport_name"] = arr_info["airport"].get("name")
                        
                        if "city" in arr_info["airport"]:
                            leg_info["arrival_city"] = arr_info["airport"]["city"].get("name")
                            
                            if "country" in arr_info["airport"]["city"]:
                                leg_info["arrival_country"] = arr_info["airport"]["city"]["country"].get("name")
                        
                        if "location" in arr_info["airport"]:
                            leg_info["arrival_latitude"] = arr_info["airport"]["location"].get("latitude")
                            leg_info["arrival_longitude"] = arr_info["airport"]["location"].get("longitude")
                    
                    # Add arrival time
                    if "times" in arr_info:
                        leg_info["scheduled_arrival_time"] = arr_info["times"].get("scheduled")
                        if "estimated" in arr_info["times"]:
                            leg_info["estimated_arrival_time"] = arr_info["times"]["estimated"].get("value")
                    
                    # Add aircraft information
                    if "aircraft" in leg:
                        leg_info["aircraft_type"] = leg["aircraft"].get("typeName")
                        leg_info["aircraft_code"] = leg["aircraft"].get("typeCode")
                    
                    # Add flight duration
                    leg_info["scheduled_duration"] = leg.get("scheduledFlightDuration")
                    
                    flights.append(leg_info)
            else:
                # If no legs, just add the flight information
                flights.append(flight_info)
        
        # Convert to DataFrame
        df = pd.DataFrame(flights)
        
        # Save as CSV
        csv_path = os.path.join(self.processed_dir, "flights.csv")
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Processed {len(flights)} flights and saved to {csv_path}")
        
        # Also extract all airports
        airports = []
        
        for flight in flights:
            # Add departure airport
            if "departure_airport_code" in flight and flight["departure_airport_code"]:
                airport = {
                    "airport_code": flight["departure_airport_code"],
                    "airport_name": flight.get("departure_airport_name"),
                    "city": flight.get("departure_city"),
                    "country": flight.get("departure_country"),
                    "latitude": flight.get("departure_latitude"),
                    "longitude": flight.get("departure_longitude")
                }
                airports.append(airport)
            
            # Add arrival airport
            if "arrival_airport_code" in flight and flight["arrival_airport_code"]:
                airport = {
                    "airport_code": flight["arrival_airport_code"],
                    "airport_name": flight.get("arrival_airport_name"),
                    "city": flight.get("arrival_city"),
                    "country": flight.get("arrival_country"),
                    "latitude": flight.get("arrival_latitude"),
                    "longitude": flight.get("arrival_longitude")
                }
                airports.append(airport)
        
        # Remove duplicates (by airport code)
        unique_airports = {}
        for airport in airports:
            code = airport["airport_code"]
            if code and code not in unique_airports:
                unique_airports[code] = airport
        
        # Convert to DataFrame
        airport_df = pd.DataFrame(list(unique_airports.values()))
        
        # Save as CSV
        airport_csv_path = os.path.join(self.processed_dir, "airports.csv")
        airport_df.to_csv(airport_csv_path, index=False)
        
        logger.info(f"Processed {len(unique_airports)} airports and saved to {airport_csv_path}")
        
        return {
            "flights": df,
            "airports": airport_df
        }
    
    def process_baggage_data(self):
        """Process baggage allowance data"""
        logger.info("Processing baggage allowance data")
        
        # Get the latest baggage file
        filepath = self.get_latest_file("klm_baggage_response")
        if not filepath:
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data:
            return None
        
        # Process baggage data (modify based on actual structure)
        # This is a placeholder - you'll need to adapt this to the actual structure
        # of the baggage response once you've collected it
        try:
            baggage_items = []
            
            if "baggageAllowances" in data:
                for item in data["baggageAllowances"]:
                    baggage_info = {
                        "baggage_type": item.get("type"),
                        "description": item.get("description"),
                        "allowance": item.get("allowance"),
                        "weight_limit": item.get("weightLimit"),
                        "dimensions": item.get("dimensions")
                    }
                    baggage_items.append(baggage_info)
            
            # Convert to DataFrame
            if baggage_items:
                df = pd.DataFrame(baggage_items)
                
                # Save as CSV
                csv_path = os.path.join(self.processed_dir, "baggage_allowances.csv")
                df.to_csv(csv_path, index=False)
                
                logger.info(f"Processed {len(baggage_items)} baggage allowances and saved to {csv_path}")
                return df
            else:
                logger.warning("No baggage data found to process")
                return None
                
        except Exception as e:
            logger.error(f"Error processing baggage data: {str(e)}")
            return None
    
    def process_inspire_data(self):
        """Process inspire/amenities data"""
        logger.info("Processing inspire/amenities data")
        
        # Get the latest inspire file
        filepath = self.get_latest_file("klm_inspire_response")
        if not filepath:
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data:
            return None
        
        # Process inspire data (modify based on actual structure)
        # This is a placeholder - you'll need to adapt this to the actual structure
        # of the inspire response once you've collected it
        try:
            amenities = []
            
            if "amenities" in data:
                for item in data["amenities"]:
                    amenity_info = {
                        "amenity_id": item.get("id"),
                        "name": item.get("name"),
                        "description": item.get("description"),
                        "category": item.get("category")
                    }
                    amenities.append(amenity_info)
            
            # Convert to DataFrame
            if amenities:
                df = pd.DataFrame(amenities)
                
                # Save as CSV
                csv_path = os.path.join(self.processed_dir, "amenities.csv")
                df.to_csv(csv_path, index=False)
                
                logger.info(f"Processed {len(amenities)} amenities and saved to {csv_path}")
                return df
            else:
                logger.warning("No amenities data found to process")
                return None
                
        except Exception as e:
            logger.error(f"Error processing inspire data: {str(e)}")
            return None
    
    def process_offers_data(self):
        """Process flight offers data"""
        logger.info("Processing flight offers data")
        
        # Get the latest offers file
        filepath = self.get_latest_file("klm_offers_response")
        if not filepath:
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data:
            return None
        
        # Process offers data (modify based on actual structure)
        # This is a placeholder - you'll need to adapt this to the actual structure
        # of the offers response once you've collected it
        try:
            offers = []
            
            if "flightOffers" in data:
                for item in data["flightOffers"]:
                    offer_info = {
                        "offer_id": item.get("id"),
                        "origin": item.get("origin"),
                        "destination": item.get("destination"),
                        "price": item.get("price"),
                        "date": item.get("date")
                    }
                    offers.append(offer_info)
            
            # Convert to DataFrame
            if offers:
                df = pd.DataFrame(offers)
                
                # Save as CSV
                csv_path = os.path.join(self.processed_dir, "flight_offers.csv")
                df.to_csv(csv_path, index=False)
                
                logger.info(f"Processed {len(offers)} flight offers and saved to {csv_path}")
                return df
            else:
                logger.warning("No flight offers data found to process")
                return None
                
        except Exception as e:
            logger.error(f"Error processing offers data: {str(e)}")
            return None
    
    def process_all(self):
        """Process all available data"""
        logger.info("Processing all KLM data")
        
        results = {}
        
        # Process flight status data
        results["flight_status"] = self.process_flight_status()
        
        # Process baggage data
        results["baggage"] = self.process_baggage_data()
        
        # Process inspire data
        results["inspire"] = self.process_inspire_data()
        
        # Process offers data
        results["offers"] = self.process_offers_data()
        
        logger.info("Completed processing all KLM data")
        return results

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process KLM API data")
    parser.add_argument("--type", choices=["all", "flight_status", "baggage", "inspire", "offers"], 
                        default="all", help="Specific data type to process")
    
    args = parser.parse_args()
    
    # Create processor
    processor = KLMDataProcessor()
    
    # Process data
    if args.type == "all":
        processor.process_all()
    elif args.type == "flight_status":
        processor.process_flight_status()
    elif args.type == "baggage":
        processor.process_baggage_data()
    elif args.type == "inspire":
        processor.process_inspire_data()
    elif args.type == "offers":
        processor.process_offers_data()

if __name__ == "__main__":
    main()