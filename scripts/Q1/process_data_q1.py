""" KLM Data Processor for Hub Analysis

This script preprocesses the raw KLM and Schiphol API data to prepare it for hub analysis.
It handles the specific format of the data from the provided samples, extracts relevant information,
and creates clean CSV files for the hub analyzer.

Usage:
    python process_klm_data.py --klm_file=path/to/klm_data.csv --schiphol_file=path/to/schiphol_data.csv --output_dir=data/Q1/raw
"""

import os
import pandas as pd
import numpy as np
import argparse
from datetime import datetime
import logging
import csv
import re
import json
import glob
from geopy.distance import great_circle

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DataProcessor")

# Constants
SCHIPHOL_CODE = "AMS"  # IATA code for Schiphol
KLM_AIRLINE_CODE = "KL"  # KLM airline code

class KLMDataProcessor:
    """Processor for KLM and Schiphol flight data"""
    
    def __init__(self, klm_raw_dir="data/KLM/raw", schiphol_raw_dir="data/Schiphol/raw", 
                 output_dir="data/Q1/processed", eurostat_file=None):
        """
        Initialize the data processor
        
        Args:
            klm_raw_dir: Directory containing raw KLM API data
            schiphol_raw_dir: Directory containing raw Schiphol API data
            output_dir: Directory to save processed data
            eurostat_file: Optional file containing Eurostat passenger data
        """
        self.klm_raw_dir = klm_raw_dir
        self.schiphol_raw_dir = schiphol_raw_dir
        self.output_dir = output_dir
        self.eurostat_file = eurostat_file
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Set up paths for processed files
        self.flights_output = os.path.join(output_dir, "flights.csv")
        self.airports_output = os.path.join(output_dir, "airports.csv")
        self.schiphol_flights_output = os.path.join(output_dir, "schiphol_flights.csv")
        self.schiphol_destinations_output = os.path.join(output_dir, "schiphol_destinations.csv")
        
        # Data stores
        self.klm_flights = None
        self.airports = None
        self.schiphol_flights = None
        self.schiphol_destinations = None
        self.eurostat_data = None
        
        logger.info("KLM Data Processor initialized")
    
    def find_latest_file(self, directory, prefix):
        """Find the latest file with the given prefix in the directory"""
        pattern = os.path.join(directory, f"{prefix}_*.json")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning(f"No files found matching pattern {pattern}")
            return None
        
        # Sort by modification time (newest first)
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found latest file: {latest_file}")
        
        return latest_file
    
    def load_klm_data(self):
        """Load raw KLM flight data"""
        logger.info("Loading KLM flight data")
        
        try:
            # Find the latest flight status file
            flightstatus_file = self.find_latest_file(self.klm_raw_dir, "klm_flightstatus_response")
            
            if not flightstatus_file:
                logger.error("No KLM flight status file found")
                return False
            
            # Load the data
            with open(flightstatus_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if "operationalFlights" not in data:
                logger.error("Flight data does not contain 'operationalFlights' key")
                return False
            
            # Extract flight information
            flights = []
            airports = set()
            
            for flight in data["operationalFlights"]:
                flight_info = {
                    "flight_number": flight.get("flightNumber"),
                    "flight_date": flight.get("flightScheduleDate"),
                    "flight_id": flight.get("id"),
                    "airline_code": flight.get("airline", {}).get("code"),
                    "airline_name": flight.get("airline", {}).get("name"),
                    "status": flight.get("flightStatusPublic")
                }
                
                # Add route information
                if "route" in flight:
                    flight_info["origin"] = flight["route"][0] if len(flight["route"]) > 0 else None
                    flight_info["destination"] = flight["route"][-1] if len(flight["route"]) > 0 else None
                
                # Process flight legs for detailed information
                if "flightLegs" in flight and flight["flightLegs"]:
                    for leg in flight["flightLegs"]:
                        leg_info = flight_info.copy()
                        
                        # Add leg status
                        leg_info["leg_status"] = leg.get("legStatusPublic")
                        
                        # Add departure information
                        dep_info = leg.get("departureInformation", {})
                        if "airport" in dep_info:
                            airport = dep_info["airport"]
                            leg_info["departure_airport_code"] = airport.get("code")
                            leg_info["departure_airport_name"] = airport.get("name")
                            
                            # Add airport to list for later processing
                            if airport.get("code"):
                                airports.add((
                                    airport.get("code"),
                                    airport.get("name"),
                                    airport.get("city", {}).get("name"),
                                    airport.get("city", {}).get("country", {}).get("name"),
                                    airport.get("location", {}).get("latitude"),
                                    airport.get("location", {}).get("longitude")
                                ))
                            
                            # Add city information
                            if "city" in airport:
                                leg_info["departure_city"] = airport["city"].get("name")
                                
                                # Add country information
                                if "country" in airport["city"]:
                                    leg_info["departure_country"] = airport["city"]["country"].get("name")
                            
                            # Add location information
                            if "location" in airport:
                                leg_info["departure_latitude"] = airport["location"].get("latitude")
                                leg_info["departure_longitude"] = airport["location"].get("longitude")
                        
                        # Add departure time
                        if "times" in dep_info:
                            leg_info["scheduled_departure_time"] = dep_info["times"].get("scheduled")
                            leg_info["latest_departure_time"] = dep_info["times"].get("latestPublished")
                        
                        # Add arrival information
                        arr_info = leg.get("arrivalInformation", {})
                        if "airport" in arr_info:
                            airport = arr_info["airport"]
                            leg_info["arrival_airport_code"] = airport.get("code")
                            leg_info["arrival_airport_name"] = airport.get("name")
                            
                            # Add airport to list for later processing
                            if airport.get("code"):
                                airports.add((
                                    airport.get("code"),
                                    airport.get("name"),
                                    airport.get("city", {}).get("name"),
                                    airport.get("city", {}).get("country", {}).get("name"),
                                    airport.get("location", {}).get("latitude"),
                                    airport.get("location", {}).get("longitude")
                                ))
                            
                            # Add city information
                            if "city" in airport:
                                leg_info["arrival_city"] = airport["city"].get("name")
                                
                                # Add country information
                                if "country" in airport["city"]:
                                    leg_info["arrival_country"] = airport["city"]["country"].get("name")
                            
                            # Add location information
                            if "location" in airport:
                                leg_info["arrival_latitude"] = airport["location"].get("latitude")
                                leg_info["arrival_longitude"] = airport["location"].get("longitude")
                        
                        # Add arrival time
                        if "times" in arr_info:
                            leg_info["scheduled_arrival_time"] = arr_info["times"].get("scheduled")
                            leg_info["latest_arrival_time"] = arr_info["times"].get("latestPublished")
                            
                            # Add estimated arrival time
                            if "estimated" in arr_info["times"]:
                                leg_info["estimated_arrival_time"] = arr_info["times"]["estimated"].get("value")
                        
                        # Add aircraft information
                        if "aircraft" in leg:
                            leg_info["aircraft_type"] = leg["aircraft"].get("typeName")
                            leg_info["aircraft_code"] = leg["aircraft"].get("typeCode")
                        
                        # Add duration
                        leg_info["scheduled_duration"] = leg.get("scheduledFlightDuration")
                        
                        flights.append(leg_info)
                else:
                    # If no legs, just add the basic flight info
                    flights.append(flight_info)
            
            # Convert to dataframes
            self.klm_flights = pd.DataFrame(flights)
            
            # Convert airports set to dataframe
            airport_data = [{
                "airport_code": code,
                "airport_name": name,
                "city": city,
                "country": country,
                "latitude": lat,
                "longitude": lon
            } for code, name, city, country, lat, lon in airports if code]
            
            self.airports = pd.DataFrame(airport_data)
            
            # Remove duplicates from airports
            if not self.airports.empty:
                self.airports = self.airports.drop_duplicates(subset=['airport_code'])
            
            logger.info(f"Loaded {len(flights)} KLM flights and {len(self.airports)} airports")
            return True
            
        except Exception as e:
            logger.error(f"Error loading KLM data: {str(e)}")
            return False
    
    def load_schiphol_data(self):
        """Load raw Schiphol data"""
        logger.info("Loading Schiphol data")
        
        try:
            # Find the latest files
            flights_file = self.find_latest_file(self.schiphol_raw_dir, "all_flights_all")
            destinations_file = self.find_latest_file(self.schiphol_raw_dir, "destinations")
            
            if not flights_file and not destinations_file:
                logger.warning("No Schiphol data files found")
                return False
            
            # Process flights data
            if flights_file:
                with open(flights_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if "flights" in data:
                    flights_data = []
                    
                    for flight in data["flights"]:
                        flight_info = {
                            "flight_name": flight.get("flightName"),
                            "flight_number": flight.get("flightNumber"),
                            "airline_code": flight.get("prefixIATA"),
                            "airline_code_icao": flight.get("prefixICAO"),
                            "flight_direction": flight.get("flightDirection"),
                            "schedule_date": flight.get("scheduleDate"),
                            "schedule_time": flight.get("scheduleTime"),
                            "terminal": flight.get("terminal"),
                            "gate": flight.get("gate"),
                            "pier": flight.get("pier"),
                            "service_type": flight.get("serviceType")
                        }
                        
                        # Extract route information
                        if "route" in flight and "destinations" in flight["route"]:
                            flight_info["destinations"] = ", ".join(flight["route"]["destinations"])
                            flight_info["eu"] = flight["route"].get("eu")
                        
                        # Extract status information
                        if "publicFlightState" in flight and "flightStates" in flight["publicFlightState"]:
                            flight_info["flight_states"] = ", ".join(flight["publicFlightState"]["flightStates"])
                        
                        # Add timing information
                        time_fields = [
                            "estimatedLandingTime", "actualLandingTime", 
                            "expectedTimeBoarding", "expectedTimeGateClosing",
                            "publicEstimatedOffBlockTime", "actualOffBlockTime"
                        ]
                        
                        for field in time_fields:
                            if field in flight:
                                flight_info[field] = flight[field]
                        
                        # Add aircraft type
                        if "aircraftType" in flight and "iataMain" in flight["aircraftType"]:
                            flight_info["aircraft_type"] = flight["aircraftType"]["iataMain"]
                        
                        flights_data.append(flight_info)
                    
                    self.schiphol_flights = pd.DataFrame(flights_data)
                    logger.info(f"Loaded {len(flights_data)} Schiphol flights")
            
            # Process destinations data
            if destinations_file:
                with open(destinations_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if "destinations" in data:
                    dest_data = []
                    
                    for dest in data["destinations"]:
                        dest_info = {
                            "iata": dest.get("iata"),
                            "city": dest.get("city"),
                            "country": dest.get("country")
                        }
                        
                        # Add public name if available
                        if "publicName" in dest:
                            dest_info["name_english"] = dest["publicName"].get("english")
                            dest_info["name_dutch"] = dest["publicName"].get("dutch")
                        
                        dest_data.append(dest_info)
                    
                    self.schiphol_destinations = pd.DataFrame(dest_data)
                    logger.info(f"Loaded {len(dest_data)} Schiphol destinations")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading Schiphol data: {str(e)}")
            return False
    
    def load_eurostat_data(self):
        """Load Eurostat passenger data if available"""
        if not self.eurostat_file or not os.path.exists(self.eurostat_file):
            logger.info("No Eurostat data file provided or file not found")
            return False
        
        logger.info(f"Loading Eurostat data from {self.eurostat_file}")
        
        try:
            # Load the data
            self.eurostat_data = pd.read_csv(self.eurostat_file)
            logger.info(f"Loaded Eurostat data with {len(self.eurostat_data)} entries")
            return True
        except Exception as e:
            logger.error(f"Error loading Eurostat data: {str(e)}")
            return False
    
    def enrich_airports_data(self):
        """Enrich airports data with additional information"""
        if self.airports is None:
            logger.warning("No airports data to enrich")
            return False
        
        logger.info("Enriching airports data")
        
        try:
            # Calculate distance from Schiphol for each airport
            schiphol_row = self.airports[self.airports['airport_code'] == SCHIPHOL_CODE]
            
            if not schiphol_row.empty:
                schiphol_coords = (
                    schiphol_row.iloc[0]['latitude'],
                    schiphol_row.iloc[0]['longitude']
                )
                
                # Function to calculate distance
                def calculate_distance(row):
                    if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                        return None
                    return great_circle(
                        (row['latitude'], row['longitude']),
                        schiphol_coords
                    ).kilometers
                
                # Add distance column
                self.airports['distance_from_ams'] = self.airports.apply(calculate_distance, axis=1)
            else:
                logger.warning(f"Schiphol airport (AMS) not found in airports data")
            
            # Add passenger volume data from Eurostat if available
            if self.eurostat_data is not None and 'airport_code' in self.eurostat_data.columns:
                # Check if there's a passenger_volume column in the Eurostat data
                if 'passenger_volume' in self.eurostat_data.columns:
                    # Create a mapping from airport code to passenger volume
                    passenger_volume_map = dict(zip(
                        self.eurostat_data['airport_code'],
                        self.eurostat_data['passenger_volume']
                    ))
                    
                    # Add passenger volume to airports data
                    self.airports['passenger_volume'] = self.airports['airport_code'].map(passenger_volume_map)
                    
                    logger.info("Added passenger volume data from Eurostat")
            
            return True
        except Exception as e:
            logger.error(f"Error enriching airports data: {str(e)}")
            return False
    
    def calculate_airport_metrics(self):
        """Calculate additional metrics for airports based on flight data"""
        if self.airports is None or self.klm_flights is None:
            logger.warning("Missing data required for calculating airport metrics")
            return False
        
        logger.info("Calculating airport metrics")
        
        try:
            # Count KLM flights by airport
            if 'departure_airport_code' in self.klm_flights.columns:
                departure_counts = self.klm_flights['departure_airport_code'].value_counts()
                self.airports['klm_departures'] = self.airports['airport_code'].map(departure_counts).fillna(0)
            
            if 'arrival_airport_code' in self.klm_flights.columns:
                arrival_counts = self.klm_flights['arrival_airport_code'].value_counts()
                self.airports['klm_arrivals'] = self.airports['airport_code'].map(arrival_counts).fillna(0)
            
            # Calculate total KLM flights
            if 'klm_departures' in self.airports.columns and 'klm_arrivals' in self.airports.columns:
                self.airports['klm_total_flights'] = self.airports['klm_departures'] + self.airports['klm_arrivals']
            
            # Calculate route diversity (unique connections)
            unique_routes = set()
            
            if 'departure_airport_code' in self.klm_flights.columns and 'arrival_airport_code' in self.klm_flights.columns:
                for _, flight in self.klm_flights.iterrows():
                    origin = flight['departure_airport_code']
                    dest = flight['arrival_airport_code']
                    if pd.notna(origin) and pd.notna(dest):
                        unique_routes.add((origin, dest))
            
            # Count unique routes for each airport
            airport_connections = {}
            for origin, dest in unique_routes:
                if origin not in airport_connections:
                    airport_connections[origin] = set()
                if dest not in airport_connections:
                    airport_connections[dest] = set()
                
                airport_connections[origin].add(dest)
                airport_connections[dest].add(origin)
            
            # Add connection count to metrics
            self.airports['route_diversity'] = self.airports['airport_code'].map(
                lambda code: len(airport_connections.get(code, set()))
            ).fillna(0)
            
            # Add delay information if available
            if 'leg_status' in self.klm_flights.columns:
                delay_status = {}
                
                for _, flight in self.klm_flights.iterrows():
                    origin = flight.get('departure_airport_code')
                    dest = flight.get('arrival_airport_code')
                    status = flight.get('leg_status')
                    
                    if pd.notna(origin) and pd.notna(status):
                        if origin not in delay_status:
                            delay_status[origin] = {'total': 0, 'delayed': 0}
                        
                        delay_status[origin]['total'] += 1
                        if status in ['DELAYED', 'DIVERTED', 'CANCELLED']:
                            delay_status[origin]['delayed'] += 1
                    
                    if pd.notna(dest) and pd.notna(status):
                        if dest not in delay_status:
                            delay_status[dest] = {'total': 0, 'delayed': 0}
                        
                        delay_status[dest]['total'] += 1
                        if status in ['DELAYED', 'DIVERTED', 'CANCELLED']:
                            delay_status[dest]['delayed'] += 1
                
                # Calculate delay rates
                for code, counts in delay_status.items():
                    if counts['total'] > 0:
                        delay_status[code]['rate'] = counts['delayed'] / counts['total']
                    else:
                        delay_status[code]['rate'] = 0
                
                # Add delay rates to metrics
                self.airports['delay_rate'] = self.airports['airport_code'].map(
                    lambda code: delay_status.get(code, {}).get('rate', 0)
                )
                
                self.airports['delay_count'] = self.airports['airport_code'].map(
                    lambda code: delay_status.get(code, {}).get('delayed', 0)
                )
                
                self.airports['total_tracked_flights'] = self.airports['airport_code'].map(
                    lambda code: delay_status.get(code, {}).get('total', 0)
                )
            
            return True
        except Exception as e:
            logger.error(f"Error calculating airport metrics: {str(e)}")
            return False
    
    def save_processed_data(self):
        """Save all processed data to CSV files"""
        logger.info("Saving processed data")
        
        try:
            # Save flights data
            if self.klm_flights is not None:
                self.klm_flights.to_csv(self.flights_output, index=False)
                logger.info(f"Saved {len(self.klm_flights)} KLM flights to {self.flights_output}")
            
            # Save airports data
            if self.airports is not None:
                self.airports.to_csv(self.airports_output, index=False)
                logger.info(f"Saved {len(self.airports)} airports to {self.airports_output}")
            
            # Save Schiphol flights data
            if self.schiphol_flights is not None:
                self.schiphol_flights.to_csv(self.schiphol_flights_output, index=False)
                logger.info(f"Saved {len(self.schiphol_flights)} Schiphol flights to {self.schiphol_flights_output}")
            
            # Save Schiphol destinations data
            if self.schiphol_destinations is not None:
                self.schiphol_destinations.to_csv(self.schiphol_destinations_output, index=False)
                logger.info(f"Saved {len(self.schiphol_destinations)} Schiphol destinations to {self.schiphol_destinations_output}")
            
            return True
        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return False
    
    def run_processing(self):
        """Run the full data processing pipeline"""
        logger.info("Starting KLM data processing")
        
        # Load KLM and Schiphol data
        klm_success = self.load_klm_data()
        schiphol_success = self.load_schiphol_data()
        
        # Load Eurostat data if provided
        if self.eurostat_file:
            self.load_eurostat_data()
        
        if klm_success:
            # Enrich airports data
            self.enrich_airports_data()
            
            # Calculate additional metrics
            self.calculate_airport_metrics()
            
            # Save processed data
            self.save_processed_data()
            
            logger.info("Data processing completed successfully")
            return True
        else:
            logger.error("Data processing failed - could not load KLM data")
            return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Process KLM and Schiphol flight data for hub analysis")
    parser.add_argument("--klm_dir", default="data/KLM/raw", help="Directory containing raw KLM API data")
    parser.add_argument("--schiphol_dir", default="data/Schiphol/raw", help="Directory containing raw Schiphol API data")
    parser.add_argument("--output_dir", default="data/Q1/processed", help="Directory to save processed data")
    parser.add_argument("--eurostat_file", help="Optional file containing Eurostat passenger data")
    
    args = parser.parse_args()
    
    processor = KLMDataProcessor(
        klm_raw_dir=args.klm_dir,
        schiphol_raw_dir=args.schiphol_dir,
        output_dir=args.output_dir,
        eurostat_file=args.eurostat_file
    )
    
    processor.run_processing()

if __name__ == "__main__":
    main()