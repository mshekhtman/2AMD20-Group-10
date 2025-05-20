"""
KLM Second Hub Analysis

This script analyzes flight data from KLM and Schiphol APIs to identify optimal
candidates for KLM's second hub expansion.

The analysis considers:
1. Geographic distribution (distance from AMS)
2. Current KLM flight volume and connectivity
3. Airport capacity and passenger traffic
4. Regional strategic value

Usage:
    python klm_hub_analyzer.py --klm_data=data/KLM/processed --schiphol_data=data/Schiphol/processed 
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse
from datetime import datetime
import math
from geopy.distance import great_circle
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hub_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("HubAnalyzer")

# Constants
SCHIPHOL_CODE = "AMS"  # IATA code for Schiphol
MIN_DISTANCE_KM = 1000  # Minimum distance from Schiphol for a second hub
KLM_AIRLINE_CODE = "KL"  # KLM airline code

class HubAnalyzer:
    """Analyzer for identifying potential second hub locations for KLM"""
    
    def __init__(self, klm_data_dir=None, schiphol_data_dir=None, output_dir="results"):
        """
        Initialize the hub analyzer
        
        Args:
            klm_data_dir: Directory containing processed KLM data
            schiphol_data_dir: Directory containing processed Schiphol data
            output_dir: Directory to save analysis results
        """
        self.klm_data_dir = klm_data_dir
        self.schiphol_data_dir = schiphol_data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize dataframes
        self.airports_df = None
        self.flights_df = None
        self.schiphol_flights_df = None
        self.schiphol_destinations_df = None
        self.aircraft_types_df = None
        
        # Results storage
        self.hub_candidates = None
        self.hub_metrics = None
    
    def load_data_from_csv(self, csv_files):
        """
        Load data directly from CSV files for testing
        
        Args:
            csv_files: Dictionary mapping dataframe names to CSV file paths
        """
        logger.info("Loading data from CSV files")
        
        for name, filepath in csv_files.items():
            try:
                if os.path.exists(filepath):
                    if name == 'airports':
                        self.airports_df = pd.read_csv(filepath)
                        logger.info(f"Loaded {len(self.airports_df)} airports from {filepath}")
                    elif name == 'flights':
                        self.flights_df = pd.read_csv(filepath)
                        logger.info(f"Loaded {len(self.flights_df)} flights from {filepath}")
                    elif name == 'schiphol_flights':
                        self.schiphol_flights_df = pd.read_csv(filepath)
                        logger.info(f"Loaded {len(self.schiphol_flights_df)} Schiphol flights from {filepath}")
                    elif name == 'schiphol_destinations':
                        self.schiphol_destinations_df = pd.read_csv(filepath)
                        logger.info(f"Loaded {len(self.schiphol_destinations_df)} Schiphol destinations from {filepath}")
                    elif name == 'aircraft_types':
                        self.aircraft_types_df = pd.read_csv(filepath)
                        logger.info(f"Loaded {len(self.aircraft_types_df)} aircraft types from {filepath}")
                else:
                    logger.warning(f"File not found: {filepath}")
            except Exception as e:
                logger.error(f"Error loading {name} from {filepath}: {str(e)}")
    
    def load_data(self):
        """Load data from processed directories"""
        logger.info("Loading data from processed directories")
        
        # Load KLM data if directory is provided
        if self.klm_data_dir:
            airports_path = os.path.join(self.klm_data_dir, "airports.csv")
            flights_path = os.path.join(self.klm_data_dir, "flights.csv")
            
            if os.path.exists(airports_path):
                self.airports_df = pd.read_csv(airports_path)
                logger.info(f"Loaded {len(self.airports_df)} airports from KLM data")
            else:
                logger.warning(f"Airports file not found: {airports_path}")
            
            if os.path.exists(flights_path):
                self.flights_df = pd.read_csv(flights_path)
                logger.info(f"Loaded {len(self.flights_df)} flights from KLM data")
            else:
                logger.warning(f"Flights file not found: {flights_path}")
        
        # Load Schiphol data if directory is provided
        if self.schiphol_data_dir:
            schiphol_flights_path = os.path.join(self.schiphol_data_dir, "schiphol_flights.csv")
            schiphol_destinations_path = os.path.join(self.schiphol_data_dir, "schiphol_destinations.csv")
            aircraft_types_path = os.path.join(self.schiphol_data_dir, "schiphol_aircraft_types.csv")
            
            if os.path.exists(schiphol_flights_path):
                self.schiphol_flights_df = pd.read_csv(schiphol_flights_path)
                logger.info(f"Loaded {len(self.schiphol_flights_df)} flights from Schiphol data")
            else:
                logger.warning(f"Schiphol flights file not found: {schiphol_flights_path}")
            
            if os.path.exists(schiphol_destinations_path):
                self.schiphol_destinations_df = pd.read_csv(schiphol_destinations_path)
                logger.info(f"Loaded {len(self.schiphol_destinations_df)} destinations from Schiphol data")
            else:
                logger.warning(f"Schiphol destinations file not found: {schiphol_destinations_path}")
            
            if os.path.exists(aircraft_types_path):
                self.aircraft_types_df = pd.read_csv(aircraft_types_path)
                logger.info(f"Loaded {len(self.aircraft_types_df)} aircraft types from Schiphol data")
            else:
                logger.warning(f"Aircraft types file not found: {aircraft_types_path}")
    
    def prepare_airports_data(self):
        """
        Prepare and clean the airports data for analysis
        """
        if self.airports_df is None:
            logger.error("No airports data available for preparation")
            return None
        
        logger.info("Preparing airports data for analysis")
        
        # Make a copy to avoid modifying the original
        airports = self.airports_df.copy()
        
        # Ensure airport code is uppercase
        if 'airport_code' in airports.columns:
            airports['airport_code'] = airports['airport_code'].str.upper()
        
        # Ensure coordinates are numeric
        for col in ['latitude', 'longitude']:
            if col in airports.columns:
                airports[col] = pd.to_numeric(airports[col], errors='coerce')
        
        return airports
    
    def prepare_flights_data(self):
        """
        Prepare and clean the flights data for analysis
        """
        if self.flights_df is None:
            logger.error("No flights data available for preparation")
            return None
        
        logger.info("Preparing flights data for analysis")
        
        # Make a copy to avoid modifying the original
        flights = self.flights_df.copy()
        
        # Ensure airport codes are uppercase
        for col in ['departure_airport_code', 'arrival_airport_code']:
            if col in flights.columns:
                flights[col] = flights[col].str.upper()
        
        # Filter to only include KLM flights if airline_code column exists
        if 'airline_code' in flights.columns:
            klm_flights = flights[flights['airline_code'] == KLM_AIRLINE_CODE]
            logger.info(f"Found {len(klm_flights)} KLM flights out of {len(flights)} total flights")
            return klm_flights
        else:
            logger.warning("No airline_code column found in flights data, using all flights")
            return flights
    
    def prepare_schiphol_data(self):
        """
        Prepare and clean the Schiphol data for analysis
        """
        if self.schiphol_flights_df is None:
            logger.warning("No Schiphol flights data available for preparation")
            return None
        
        logger.info("Preparing Schiphol data for analysis")
        
        # Make a copy to avoid modifying the original
        schiphol_flights = self.schiphol_flights_df.copy()
        
        # Filter to only include KLM flights
        if 'airline_code' in schiphol_flights.columns:
            klm_schiphol_flights = schiphol_flights[schiphol_flights['airline_code'] == KLM_AIRLINE_CODE]
            logger.info(f"Found {len(klm_schiphol_flights)} KLM flights in Schiphol data out of {len(schiphol_flights)} total flights")
            return klm_schiphol_flights
        else:
            logger.warning("No airline_code column found in Schiphol flights data, using all flights")
            return schiphol_flights
    
    def calculate_airport_metrics(self):
        """
        Calculate metrics for each airport to evaluate hub potential
        """
        logger.info("Calculating airport metrics for hub analysis")
        
        # Get prepared data
        airports = self.prepare_airports_data()
        if airports is None or len(airports) == 0:
            logger.error("No airport data available for analysis")
            return None
        
        klm_flights = self.prepare_flights_data()
        schiphol_flights = self.prepare_schiphol_data()
        
        # Find Schiphol's coordinates
        schiphol_row = airports[airports['airport_code'] == SCHIPHOL_CODE]
        if len(schiphol_row) == 0:
            logger.warning(f"Schiphol (AMS) not found in airport data. Using default coordinates.")
            schiphol_coords = (52.308613, 4.763889)  # Fallback coordinates for Schiphol
        else:
            schiphol_coords = (
                schiphol_row.iloc[0]['latitude'],
                schiphol_row.iloc[0]['longitude']
            )
        
        logger.info(f"Using Schiphol coordinates: {schiphol_coords}")
        
        # Create metrics dataframe starting with airport info
        metrics = airports.copy()
        
        # Calculate distance from Schiphol for each airport
        def calculate_distance(row):
            if pd.isna(row['latitude']) or pd.isna(row['longitude']):
                return None
            return great_circle(
                (row['latitude'], row['longitude']),
                schiphol_coords
            ).kilometers
        
        metrics['distance_from_ams'] = metrics.apply(calculate_distance, axis=1)
        
        # Count flights by airport
        if klm_flights is not None:
            # Process departure counts
            if 'departure_airport_code' in klm_flights.columns:
                departure_counts = klm_flights['departure_airport_code'].value_counts()
                metrics['klm_departures'] = metrics['airport_code'].map(departure_counts).fillna(0)
            
            # Process arrival counts
            if 'arrival_airport_code' in klm_flights.columns:
                arrival_counts = klm_flights['arrival_airport_code'].value_counts()
                metrics['klm_arrivals'] = metrics['airport_code'].map(arrival_counts).fillna(0)
            
            # Calculate total KLM flights
            if 'klm_departures' in metrics.columns and 'klm_arrivals' in metrics.columns:
                metrics['klm_total_flights'] = metrics['klm_departures'] + metrics['klm_arrivals']
        
        # Add Schiphol flight data if available
        if schiphol_flights is not None:
            if 'destinations' in schiphol_flights.columns:
                # Count occurrences of each destination
                schiphol_destinations = []
                for dests in schiphol_flights['destinations'].dropna():
                    # Handle multiple destinations in one flight
                    for dest in str(dests).split(', '):
                        schiphol_destinations.append(dest)
                
                schiphol_dest_counts = pd.Series(schiphol_destinations).value_counts()
                metrics['schiphol_flight_count'] = metrics['airport_code'].map(schiphol_dest_counts).fillna(0)
        
        # Calculate route diversity (unique connections)
        if klm_flights is not None:
            unique_routes = set()
            
            if 'departure_airport_code' in klm_flights.columns and 'arrival_airport_code' in klm_flights.columns:
                for _, flight in klm_flights.iterrows():
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
            metrics['route_diversity'] = metrics['airport_code'].map(
                lambda code: len(airport_connections.get(code, set()))
            ).fillna(0)
        
        # Add delay information if available
        if klm_flights is not None and 'leg_status' in klm_flights.columns:
            delay_status = {}
            
            for _, flight in klm_flights.iterrows():
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
            metrics['delay_rate'] = metrics['airport_code'].map(
                lambda code: delay_status.get(code, {}).get('rate', 0)
            )
            
            metrics['delay_count'] = metrics['airport_code'].map(
                lambda code: delay_status.get(code, {}).get('delayed', 0)
            )
            
            metrics['total_tracked_flights'] = metrics['airport_code'].map(
                lambda code: delay_status.get(code, {}).get('total', 0)
            )
        
        # Calculate a hub potential score
        metrics['hub_potential_score'] = 0.0
        
        # Only consider airports far enough from Schiphol to be a viable second hub
        metrics.loc[metrics['distance_from_ams'] < MIN_DISTANCE_KM, 'hub_potential_score'] = 0.0
        
        # For airports beyond the minimum distance, calculate score based on available metrics
        valid_airports = metrics[metrics['distance_from_ams'] >= MIN_DISTANCE_KM].index
        
        if len(valid_airports) > 0:
            # Calculate score components
            
            # 1. Flight volume (40%)
            if 'klm_total_flights' in metrics.columns:
                max_flights = metrics['klm_total_flights'].max()
                if max_flights > 0:
                    metrics.loc[valid_airports, 'flight_volume_score'] = 0.4 * (metrics.loc[valid_airports, 'klm_total_flights'] / max_flights)
            
            # 2. Route diversity (30%)
            if 'route_diversity' in metrics.columns:
                max_diversity = metrics['route_diversity'].max()
                if max_diversity > 0:
                    metrics.loc[valid_airports, 'diversity_score'] = 0.3 * (metrics.loc[valid_airports, 'route_diversity'] / max_diversity)
            
            # 3. Inverse delay rate (10%) - lower delay rate is better
            if 'delay_rate' in metrics.columns:
                max_delay_rate = metrics['delay_rate'].max()
                if max_delay_rate > 0:
                    metrics.loc[valid_airports, 'reliability_score'] = 0.1 * (1 - (metrics.loc[valid_airports, 'delay_rate'] / max_delay_rate))
            
            # 4. Geographic distribution (20%) - prefer airports at optimal distances
            if 'distance_from_ams' in metrics.columns:
                # Normalize distance with a bell curve that peaks around 3000-5000 km
                metrics['distance_score'] = metrics['distance_from_ams'].apply(
                    lambda d: np.exp(-0.5 * ((d - 4000) / 2000) ** 2) if not pd.isna(d) else 0
                )
                metrics.loc[valid_airports, 'geographic_score'] = 0.2 * metrics.loc[valid_airports, 'distance_score']
            
            # Combine scores
            score_columns = [col for col in ['flight_volume_score', 'diversity_score', 'reliability_score', 'geographic_score'] 
                            if col in metrics.columns]
            
            if score_columns:
                metrics.loc[valid_airports, 'hub_potential_score'] = metrics.loc[valid_airports, score_columns].sum(axis=1)
        
        # Store the result
        self.hub_metrics = metrics
        
        # Sort by hub potential score and return top candidates
        self.hub_candidates = metrics.sort_values('hub_potential_score', ascending=False)
        
        logger.info(f"Calculated metrics for {len(metrics)} airports")
        return self.hub_candidates
    
    def analyze_top_candidates(self, top_n=10):
        """
        Analyze the top N candidate airports in detail
        
        Args:
            top_n: Number of top candidates to analyze
        """
        if self.hub_candidates is None:
            self.calculate_airport_metrics()
            
        if self.hub_candidates is None:
            logger.error("Unable to calculate hub metrics")
            return None
        
        # Get top N candidates
        top_candidates = self.hub_candidates.head(top_n)
        
        print(f"\nTop {top_n} Hub Candidates:")
        print("---------------------------")
        
        for idx, row in top_candidates.iterrows():
            print(f"Airport: {row['airport_name']} ({row['airport_code']})")
            print(f"Location: {row.get('city', 'Unknown')}, {row.get('country', 'Unknown')}")
            
            if pd.notna(row.get('distance_from_ams')):
                print(f"Distance from Schiphol: {row['distance_from_ams']:.1f} km")
            
            if 'klm_total_flights' in row and pd.notna(row['klm_total_flights']):
                print(f"KLM Flights: {int(row['klm_total_flights'])}")
            
            if 'route_diversity' in row and pd.notna(row['route_diversity']):
                print(f"Route Diversity: {int(row['route_diversity'])} connections")
            
            if 'delay_rate' in row and pd.notna(row['delay_rate']):
                print(f"Delay Rate: {row['delay_rate']:.2%}")
            
            print(f"Hub Potential Score: {row['hub_potential_score']:.4f}")
            print("---------------------------")
        
        return top_candidates
    
    def visualize_hub_candidates(self, top_n=20):
        """
        Create visualizations of hub candidates
        
        Args:
            top_n: Number of top candidates to visualize
        """
        if self.hub_candidates is None:
            self.calculate_airport_metrics()
            
        if self.hub_candidates is None or len(self.hub_candidates) == 0:
            logger.error("No hub candidates to visualize")
            return
        
        # Get top N candidates
        top_candidates = self.hub_candidates.head(top_n)
        
        # Create figure with subplots
        plt.figure(figsize=(16, 12))
        
        # Plot 1: Hub Score vs Distance
        plt.subplot(2, 2, 1)
        plt.scatter(
            top_candidates['distance_from_ams'], 
            top_candidates['hub_potential_score'],
            s=100,
            alpha=0.7
        )
        
        # Add airport codes as labels
        for idx, row in top_candidates.iterrows():
            plt.annotate(
                row['airport_code'],
                (row['distance_from_ams'], row['hub_potential_score']),
                fontsize=9
            )
        
        plt.xlabel('Distance from Schiphol (km)')
        plt.ylabel('Hub Potential Score')
        plt.title('Hub Potential vs. Distance')
        plt.grid(True, alpha=0.3)
        
        # Plot 2: Flight Volume
        plt.subplot(2, 2, 2)
        if 'klm_total_flights' in top_candidates.columns:
            # Sort by total flights for better visualization
            plot_df = top_candidates.sort_values('klm_total_flights', ascending=False).head(10)
            
            plt.bar(plot_df['airport_code'], plot_df['klm_total_flights'])
            plt.xlabel('Airport')
            plt.ylabel('Number of KLM Flights')
            plt.title('KLM Flight Volume')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
        else:
            plt.text(0.5, 0.5, 'Flight volume data not available',
                     horizontalalignment='center', verticalalignment='center')
        
        # Plot 3: Route Diversity
        plt.subplot(2, 2, 3)
        if 'route_diversity' in top_candidates.columns:
            # Sort by route diversity for better visualization
            plot_df = top_candidates.sort_values('route_diversity', ascending=False).head(10)
            
            plt.bar(plot_df['airport_code'], plot_df['route_diversity'])
            plt.xlabel('Airport')
            plt.ylabel('Number of Unique Connections')
            plt.title('Route Diversity')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
        else:
            plt.text(0.5, 0.5, 'Route diversity data not available',
                     horizontalalignment='center', verticalalignment='center')
        
        # Plot 4: Delay Rates
        plt.subplot(2, 2, 4)
        if 'delay_rate' in top_candidates.columns:
            # Sort by delay rate for better visualization
            plot_df = top_candidates.sort_values('delay_rate', ascending=True).head(10)
            
            plt.bar(plot_df['airport_code'], plot_df['delay_rate'] * 100)
            plt.xlabel('Airport')
            plt.ylabel('Delay Rate (%)')
            plt.title('Operational Reliability')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
        else:
            plt.text(0.5, 0.5, 'Delay rate data not available',
                     horizontalalignment='center', verticalalignment='center')
        
        plt.tight_layout()
        
        # Save the figure
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(self.output_dir, f'hub_analysis_{timestamp}.png')
        plt.savefig(output_path)
        
        logger.info(f"Visualization saved to {output_path}")
        
        plt.show()
    
    def export_results(self):
        """
        Export analysis results to CSV and JSON for further analysis
        """
        if self.hub_candidates is None:
            logger.error("No hub candidates to export")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export full metrics to CSV
        csv_path = os.path.join(self.output_dir, f'hub_metrics_{timestamp}.csv')
        self.hub_metrics.to_csv(csv_path, index=False)
        logger.info(f"Hub metrics exported to {csv_path}")
        
        # Export top candidates to JSON
        json_path = os.path.join(self.output_dir, f'top_hub_candidates_{timestamp}.json')
        
        # Get top 20 candidates
        top_candidates = self.hub_candidates.head(20)
        
        # Clean DataFrame for JSON conversion (remove NaN values)
        top_candidates_clean = top_candidates.copy()
        for col in top_candidates_clean.columns:
            if top_candidates_clean[col].dtype == 'float64':
                top_candidates_clean[col] = top_candidates_clean[col].fillna(0.0)
            else:
                top_candidates_clean[col] = top_candidates_clean[col].fillna('')
        
        # Convert to JSON
        with open(json_path, 'w') as f:
            json.dump(json.loads(top_candidates_clean.to_json(orient='records')), f, indent=2)
        
        logger.info(f"Top hub candidates exported to {json_path}")
    
    def run_analysis(self):
        """
        Run the full hub analysis pipeline
        """
        logger.info("Starting KLM second hub analysis")
        
        # Load data
        self.load_data()
        
        # Calculate metrics
        hub_candidates = self.calculate_airport_metrics()
        
        if hub_candidates is not None and len(hub_candidates) > 0:
            # Analyze top candidates
            self.analyze_top_candidates()
            
            # Visualize results
            self.visualize_hub_candidates()
            
            # Export results
            self.export_results()
            
            logger.info("Hub analysis completed successfully")
        else:
            logger.error("Hub analysis failed - no valid candidates found")

def main():
    """Main function"""
    # Hardcoded paths for data files and output directory
    klm_data_dir = "data/Q1/processed"
    schiphol_data_dir = "data/Q1/processed"
    output_dir = "data/Q1/results"
    
    # Hardcoded paths for specific CSV files
    airports_csv = "data/Q1/processed/airports.csv"
    flights_csv = "data/Q1/processed/flights.csv"
    schiphol_flights_csv = "data/Q1/processed/schiphol_flights.csv"
    schiphol_destinations_csv = "data/Q1/processed/schiphol_destinations.csv"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create analyzer instance with hardcoded paths
    analyzer = HubAnalyzer(
        klm_data_dir=klm_data_dir,
        schiphol_data_dir=schiphol_data_dir,
        output_dir=output_dir
    )
    
    # Load data directly from hardcoded CSV paths
    csv_files = {
        'airports': airports_csv,
        'flights': flights_csv,
        'schiphol_flights': schiphol_flights_csv,
        'schiphol_destinations': schiphol_destinations_csv
    }
    
    analyzer.load_data_from_csv(csv_files)
    
    # Run the analysis
    analyzer.run_analysis()

if __name__ == "__main__":
    main()