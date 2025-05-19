"""
Schiphol API Data Processor

This script processes raw Schiphol API data into structured formats for the knowledge graph.
"""

import json
import os
import pandas as pd
import logging
from datetime import datetime
import glob

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("schiphol_data_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SchipholProcessor")

class SchipholDataProcessor:
    """Processor for Schiphol API data"""
    
    def __init__(self, raw_dir='data/Schiphol/raw', processed_dir='data/Schiphol/processed'):
        """Initialize the Schiphol data processor"""
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        os.makedirs(processed_dir, exist_ok=True)
        
        logger.info("Schiphol Data Processor initialized")
    
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
    
    def process_flights(self):
        """Process flight data from Schiphol API"""
        logger.info("Processing Schiphol flight data")
        
        # Get the latest flight file (for all flights)
        filepath = self.get_latest_file("all_flights_all")
        
        # If not found, try to get departure and arrival flights separately
        if not filepath:
            logger.info("No combined flight file found, trying departure flights")
            filepath = self.get_latest_file("all_flights_departure")
            
            if not filepath:
                logger.info("No departure flight file found, trying arrival flights")
                filepath = self.get_latest_file("all_flights_arrival")
        
        if not filepath:
            logger.warning("No flight data files found")
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data or "flights" not in data:
            logger.warning("No flight data found in file")
            return None
        
        # Extract flight information
        flights = []
        
        for flight in data["flights"]:
            flight_info = {
                "id": flight.get("id"),
                "flight_name": flight.get("flightName"),
                "flight_number": flight.get("flightNumber"),
                "airline_code": flight.get("prefixIATA"),
                "airline_code_icao": flight.get("prefixICAO"),
                "flight_direction": flight.get("flightDirection"),
                "schedule_date": flight.get("scheduleDate"),
                "schedule_time": flight.get("scheduleTime"),
                "schedule_datetime": flight.get("scheduleDateTime"),
                "terminal": flight.get("terminal"),
                "gate": flight.get("gate"),
                "pier": flight.get("pier"),
                "service_type": flight.get("serviceType"),
                "main_flight": flight.get("mainFlight"),
                "last_updated": flight.get("lastUpdatedAt")
            }
            
            # Add public flight state
            if "publicFlightState" in flight and "flightStates" in flight["publicFlightState"]:
                flight_info["flight_states"] = ", ".join(flight["publicFlightState"]["flightStates"])
            
            # Extract route information
            if "route" in flight and "destinations" in flight["route"]:
                flight_info["destinations"] = ", ".join(flight["route"]["destinations"])
                flight_info["eu"] = flight["route"].get("eu")
                flight_info["visa_required"] = flight["route"].get("visa")
            
            # Add timing information
            for time_field in [
                "estimatedLandingTime", "actualLandingTime", 
                "expectedTimeGateOpen", "expectedTimeBoarding", 
                "expectedTimeGateClosing", "publicEstimatedOffBlockTime",
                "actualOffBlockTime", "expectedTimeOnBelt"
            ]:
                if time_field in flight:
                    # Convert camelCase to snake_case for field names
                    snake_case = ''.join(['_' + c.lower() if c.isupper() else c for c in time_field]).lstrip('_')
                    flight_info[snake_case] = flight[time_field]
            
            # Add aircraft information
            if "aircraftType" in flight and "iataMain" in flight["aircraftType"]:
                flight_info["aircraft_type"] = flight["aircraftType"]["iataMain"]
                if "iataMain" in flight["aircraftType"]:
                    flight_info["aircraft_subtype"] = flight["aircraftType"]["iataMain"]
            
            # Add aircraft registration
            flight_info["aircraft_registration"] = flight.get("aircraftRegistration")
            
            # Add baggage information
            if "baggageClaim" in flight and "belts" in flight["baggageClaim"]:
                flight_info["baggage_belts"] = ", ".join(flight["baggageClaim"]["belts"])
            
            # Add checkin information
            if "checkinAllocations" in flight and "checkinAllocations" in flight["checkinAllocations"]:
                checkin_desks = []
                for allocation in flight["checkinAllocations"]["checkinAllocations"]:
                    if "rows" in allocation and "rows" in allocation["rows"]:
                        for row in allocation["rows"]["rows"]:
                            if "desks" in row and "desks" in row["desks"]:
                                for desk in row["desks"]["desks"]:
                                    if "position" in desk:
                                        checkin_desks.append(str(desk["position"]))
                
                if checkin_desks:
                    flight_info["checkin_desks"] = ", ".join(checkin_desks)
            
            # Add codeshare information
            if "codeshares" in flight and "codeshares" in flight["codeshares"]:
                flight_info["codeshares"] = ", ".join(flight["codeshares"]["codeshares"])
            
            flights.append(flight_info)
        
        # Convert to DataFrame
        df = pd.DataFrame(flights)
        
        # Save as CSV
        csv_path = os.path.join(self.processed_dir, "schiphol_flights.csv")
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Processed {len(flights)} Schiphol flights and saved to {csv_path}")
        
        return df
    
    def process_destinations(self):
        """Process destination data from Schiphol API"""
        logger.info("Processing Schiphol destination data")
        
        # Get the latest destinations file
        filepath = self.get_latest_file("destinations")
        if not filepath:
            logger.warning("No destination data file found")
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data or "destinations" not in data:
            logger.warning("No destination data found in file")
            return None
        
        # Extract destination information
        destinations = []
        
        for destination in data["destinations"]:
            dest_info = {
                "iata": destination.get("iata"),
                "city": destination.get("city"),
                "country": destination.get("country")
            }
            
            # Add public name if available
            if "publicName" in destination:
                dest_info["name_english"] = destination["publicName"].get("english")
                dest_info["name_dutch"] = destination["publicName"].get("dutch")
            
            destinations.append(dest_info)
        
        # Convert to DataFrame
        df = pd.DataFrame(destinations)
        
        # Save as CSV
        csv_path = os.path.join(self.processed_dir, "schiphol_destinations.csv")
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Processed {len(destinations)} Schiphol destinations and saved to {csv_path}")
        
        return df
    
    def process_airlines(self):
        """Process airline data from Schiphol API"""
        logger.info("Processing Schiphol airline data")
        
        # Get the latest airlines file
        filepath = self.get_latest_file("airlines")
        if not filepath:
            logger.warning("No airline data file found")
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data or "airlines" not in data:
            logger.warning("No airline data found in file")
            return None
        
        # Extract airline information
        airlines = []
        
        for airline in data["airlines"]:
            airline_info = {
                "iata": airline.get("iata"),
                "icao": airline.get("icao"),
                "nvls": airline.get("nvls"),
                "public_name": airline.get("publicName")
            }
            
            airlines.append(airline_info)
        
        # Convert to DataFrame
        df = pd.DataFrame(airlines)
        
        # Save as CSV
        csv_path = os.path.join(self.processed_dir, "schiphol_airlines.csv")
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Processed {len(airlines)} Schiphol airlines and saved to {csv_path}")
        
        return df
    
    def process_aircraft_types(self):
        """Process aircraft type data from Schiphol API"""
        logger.info("Processing Schiphol aircraft type data")
        
        # Get the latest aircraft types file
        filepath = self.get_latest_file("aircraft_types")
        if not filepath:
            logger.warning("No aircraft type data file found")
            return None
        
        # Load the data
        data = self.load_json_data(filepath)
        if not data or "aircraftTypes" not in data:
            logger.warning("No aircraft type data found in file")
            return None
        
        # Extract aircraft type information
        aircraft_types = []
        
        for aircraft_type in data["aircraftTypes"]:
            type_info = {
                "iata_main": aircraft_type.get("iataMain"),
                "iata_sub": aircraft_type.get("iataSub"),
                "short_description": aircraft_type.get("shortDescription"),
                "long_description": aircraft_type.get("longDescription")
            }
            
            aircraft_types.append(type_info)
        
        # Convert to DataFrame
        df = pd.DataFrame(aircraft_types)
        
        # Save as CSV
        csv_path = os.path.join(self.processed_dir, "schiphol_aircraft_types.csv")
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Processed {len(aircraft_types)} Schiphol aircraft types and saved to {csv_path}")
        
        return df
    
    def process_merged_flight_data(self):
        """Create a merged dataset with flights and their associated data"""
        logger.info("Creating merged flight dataset")
        
        # Load processed datasets
        flights_path = os.path.join(self.processed_dir, "schiphol_flights.csv")
        airlines_path = os.path.join(self.processed_dir, "schiphol_airlines.csv")
        destinations_path = os.path.join(self.processed_dir, "schiphol_destinations.csv")
        aircraft_path = os.path.join(self.processed_dir, "schiphol_aircraft_types.csv")
        
        if not os.path.exists(flights_path):
            logger.warning("Flights data not found, processing flights first")
            self.process_flights()
            if not os.path.exists(flights_path):
                logger.error("Failed to process flights data")
                return None
        
        # Load datasets
        flights_df = pd.read_csv(flights_path)
        
        # Create mapping dictionaries for lookups
        airlines_dict = {}
        destinations_dict = {}
        aircraft_dict = {}
        
        # Load airlines if available
        if os.path.exists(airlines_path):
            airlines_df = pd.read_csv(airlines_path)
            airlines_dict = airlines_df.set_index('iata').to_dict('index')
        else:
            logger.warning("Airlines data not found")
        
        # Load destinations if available
        if os.path.exists(destinations_path):
            destinations_df = pd.read_csv(destinations_path)
            destinations_dict = destinations_df.set_index('iata').to_dict('index')
        else:
            logger.warning("Destinations data not found")
        
        # Load aircraft types if available
        if os.path.exists(aircraft_path):
            aircraft_df = pd.read_csv(aircraft_path)
            aircraft_dict = aircraft_df.set_index('iata_main').to_dict('index')
        else:
            logger.warning("Aircraft types data not found")
        
        # Enrich flight data with associated information
        enriched_flights = []
        
        for _, flight in flights_df.iterrows():
            flight_data = flight.to_dict()
            
            # Add airline information
            if flight_data.get('airline_code') in airlines_dict:
                airline_info = airlines_dict[flight_data['airline_code']]
                for key, value in airline_info.items():
                    if key != 'iata':  # Avoid duplicate
                        flight_data[f'airline_{key}'] = value
            
            # Add aircraft information
            if flight_data.get('aircraft_type') in aircraft_dict:
                aircraft_info = aircraft_dict[flight_data['aircraft_type']]
                for key, value in aircraft_info.items():
                    if key != 'iata_main':  # Avoid duplicate
                        flight_data[f'aircraft_{key}'] = value
            
            # Add destination information (for first destination if multiple)
            if flight_data.get('destinations'):
                dest_codes = flight_data['destinations'].split(', ')
                if dest_codes and dest_codes[0] in destinations_dict:
                    dest_info = destinations_dict[dest_codes[0]]
                    for key, value in dest_info.items():
                        if key != 'iata':  # Avoid duplicate
                            flight_data[f'destination_{key}'] = value
            
            enriched_flights.append(flight_data)
        
        # Convert to DataFrame
        enriched_df = pd.DataFrame(enriched_flights)
        
        # Save as CSV
        csv_path = os.path.join(self.processed_dir, "schiphol_flights_enriched.csv")
        enriched_df.to_csv(csv_path, index=False)
        
        logger.info(f"Created enriched flight dataset with {len(enriched_flights)} flights and saved to {csv_path}")
        
        return enriched_df
    
    def process_all(self):
        """Process all available Schiphol data"""
        logger.info("Processing all Schiphol data")
        
        results = {}
        
        # Process flights
        results["flights"] = self.process_flights()
        
        # Process destinations
        results["destinations"] = self.process_destinations()
        
        # Process airlines
        results["airlines"] = self.process_airlines()
        
        # Process aircraft types
        results["aircraft_types"] = self.process_aircraft_types()
        
        # Create merged dataset
        results["merged"] = self.process_merged_flight_data()
        
        logger.info("Completed processing all Schiphol data")
        return results

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Process Schiphol API data")
    parser.add_argument("--type", choices=["all", "flights", "destinations", "airlines", "aircraft_types", "merged"], 
                        default="all", help="Specific data type to process")
    
    args = parser.parse_args()
    
    # Create processor
    processor = SchipholDataProcessor()
    
    # Process data
    if args.type == "all":
        processor.process_all()
    elif args.type == "flights":
        processor.process_flights()
    elif args.type == "destinations":
        processor.process_destinations()
    elif args.type == "airlines":
        processor.process_airlines()
    elif args.type == "aircraft_types":
        processor.process_aircraft_types()
    elif args.type == "merged":
        processor.process_merged_flight_data()

if __name__ == "__main__":
    main()