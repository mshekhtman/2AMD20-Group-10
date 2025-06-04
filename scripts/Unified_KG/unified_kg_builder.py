"""
Unified KLM-Schiphol Knowledge Graph Builder

This script creates a single, unified knowledge graph from both KLM and Schiphol
processed data, avoiding the redundancy of separate graph creation and merging.
"""

import os
import pandas as pd
import logging
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, RDFS, XSD, OWL
import re
from datetime import datetime
import numpy as np
from typing import Dict, Set, Optional, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("unified_kg_builder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("UnifiedKGBuilder")

class UnifiedKnowledgeGraphBuilder:
    """Unified Knowledge Graph Builder for KLM Hub Expansion Analysis"""
    
    def __init__(self, klm_processed_dir='data/KLM/processed', 
                 schiphol_processed_dir='data/Schiphol/processed',
                 output_dir='data/knowledge_graph'):
        """Initialize the unified knowledge graph builder"""
        self.klm_processed_dir = klm_processed_dir
        self.schiphol_processed_dir = schiphol_processed_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize single RDF graph
        self.graph = Graph()
        
        # Define namespaces
        self.klm = Namespace("http://example.org/klm/")
        self.sch = Namespace("http://example.org/schiphol/")
        self.schema = Namespace("http://schema.org/")
        self.geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        self.dbo = Namespace("http://dbpedia.org/ontology/")
        
        # Bind namespaces
        self.graph.bind("klm", self.klm)
        self.graph.bind("sch", self.sch)
        self.graph.bind("schema", self.schema)
        self.graph.bind("geo", self.geo)
        self.graph.bind("dbo", self.dbo)
        self.graph.bind("owl", OWL)
        self.graph.bind("rdfs", RDFS)
        
        # Entity tracking for deduplication
        self.airports: Dict[str, URIRef] = {}
        self.airlines: Dict[str, URIRef] = {}
        self.cities: Dict[str, URIRef] = {}
        self.countries: Dict[str, URIRef] = {}
        self.routes: Dict[str, URIRef] = {}
        self.aircraft: Dict[str, URIRef] = {}
        
        # Metrics tracking for analysis
        self.airport_metrics: Dict[str, Dict] = {}
        self.route_metrics: Dict[str, Dict] = {}
        
        logger.info("Unified Knowledge Graph Builder initialized")
    
    def load_all_data(self) -> Dict:
        """Load all processed data from both KLM and Schiphol sources"""
        logger.info("Loading all processed data")
        
        data = {}
        
        # Load KLM data
        klm_files = {
            'klm_flights': 'flights.csv',
            'klm_airports': 'airports.csv', 
            'klm_airlines': 'airlines.csv',
            'klm_routes': 'routes.csv'
        }
        
        for key, filename in klm_files.items():
            filepath = os.path.join(self.klm_processed_dir, filename)
            if os.path.exists(filepath):
                data[key] = pd.read_csv(filepath)
                logger.info(f"Loaded {len(data[key])} records from {filename}")
            else:
                logger.warning(f"KLM file not found: {filepath}")
                data[key] = pd.DataFrame()
        
        # Load Schiphol data
        schiphol_files = {
            'sch_flights': 'schiphol_flights.csv',
            'sch_flights_enriched': 'schiphol_flights_enriched.csv',
            'sch_destinations': 'schiphol_destinations.csv',
            'sch_airlines': 'schiphol_airlines.csv',
            'sch_aircraft_types': 'schiphol_aircraft_types.csv'
        }
        
        for key, filename in schiphol_files.items():
            filepath = os.path.join(self.schiphol_processed_dir, filename)
            if os.path.exists(filepath):
                data[key] = pd.read_csv(filepath)
                logger.info(f"Loaded {len(data[key])} records from {filename}")
            else:
                logger.warning(f"Schiphol file not found: {filepath}")
                data[key] = pd.DataFrame()
        
        return data
    
    def create_unified_ontology(self):
        """Create the unified ontology incorporating both KLM and Schiphol concepts"""
        logger.info("Creating unified ontology")
        
        # Core Classes
        airport_class = self.klm.Airport
        self.graph.add((airport_class, RDF.type, OWL.Class))
        self.graph.add((airport_class, RDFS.label, Literal("Airport")))
        
        hub_airport_class = self.klm.HubAirport
        self.graph.add((hub_airport_class, RDF.type, OWL.Class))
        self.graph.add((hub_airport_class, RDFS.subClassOf, airport_class))
        self.graph.add((hub_airport_class, RDFS.label, Literal("Hub Airport")))
        
        potential_hub_class = self.klm.PotentialHubAirport
        self.graph.add((potential_hub_class, RDF.type, OWL.Class))
        self.graph.add((potential_hub_class, RDFS.subClassOf, airport_class))
        self.graph.add((potential_hub_class, RDFS.label, Literal("Potential Hub Airport")))
        
        airline_class = self.klm.Airline
        self.graph.add((airline_class, RDF.type, OWL.Class))
        self.graph.add((airline_class, RDFS.label, Literal("Airline")))
        
        route_class = self.klm.Route
        self.graph.add((route_class, RDF.type, OWL.Class))
        self.graph.add((route_class, RDFS.label, Literal("Route")))
        
        flight_class = self.klm.Flight
        self.graph.add((flight_class, RDF.type, OWL.Class))
        self.graph.add((flight_class, RDFS.label, Literal("Flight")))
        
        # Schiphol-specific classes
        schiphol_flight_class = self.sch.SchipholFlight
        self.graph.add((schiphol_flight_class, RDF.type, OWL.Class))
        self.graph.add((schiphol_flight_class, RDFS.subClassOf, flight_class))
        
        # Geographic classes
        city_class = self.klm.City
        self.graph.add((city_class, RDF.type, OWL.Class))
        self.graph.add((city_class, RDFS.label, Literal("City")))
        
        country_class = self.klm.Country
        self.graph.add((country_class, RDF.type, OWL.Class))
        self.graph.add((country_class, RDFS.label, Literal("Country")))
        
        aircraft_class = self.klm.Aircraft
        self.graph.add((aircraft_class, RDF.type, OWL.Class))
        self.graph.add((aircraft_class, RDFS.label, Literal("Aircraft")))
        
        # Infrastructure classes
        terminal_class = self.sch.Terminal
        self.graph.add((terminal_class, RDF.type, OWL.Class))
        self.graph.add((terminal_class, RDFS.label, Literal("Terminal")))
        
        gate_class = self.sch.Gate
        self.graph.add((gate_class, RDF.type, OWL.Class))
        self.graph.add((gate_class, RDFS.label, Literal("Gate")))
        
        pier_class = self.sch.Pier
        self.graph.add((pier_class, RDF.type, OWL.Class))
        self.graph.add((pier_class, RDFS.label, Literal("Pier")))
        
        # Define key properties
        self._define_properties()
        
        logger.info("Unified ontology created")
    
    def _define_properties(self):
        """Define data and object properties for the unified ontology"""
        
        # Core identification properties
        code_prop = self.klm.code
        self.graph.add((code_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((code_prop, RDFS.label, Literal("code")))
        
        name_prop = self.klm.name
        self.graph.add((name_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((name_prop, RDFS.label, Literal("name")))
        
        # Geographic properties
        self.graph.add((self.geo.lat, RDF.type, OWL.DatatypeProperty))
        self.graph.add((self.geo.long, RDF.type, OWL.DatatypeProperty))
        
        # Hub analysis properties
        hub_score_prop = self.klm.hubPotentialScore
        self.graph.add((hub_score_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((hub_score_prop, RDFS.label, Literal("hub potential score")))
        
        connectivity_prop = self.klm.connectivityIndex
        self.graph.add((connectivity_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((connectivity_prop, RDFS.label, Literal("connectivity index")))
        
        passenger_vol_prop = self.klm.passengerVolume
        self.graph.add((passenger_vol_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((passenger_vol_prop, RDFS.label, Literal("passenger volume")))
        
        route_count_prop = self.klm.routeCount
        self.graph.add((route_count_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((route_count_prop, RDFS.label, Literal("route count")))
        
        delay_rate_prop = self.klm.delayRate
        self.graph.add((delay_rate_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((delay_rate_prop, RDFS.label, Literal("delay rate")))
        
        # Object properties
        located_in_prop = self.klm.locatedIn
        self.graph.add((located_in_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((located_in_prop, RDFS.label, Literal("located in")))
        
        has_origin_prop = self.klm.hasOrigin
        self.graph.add((has_origin_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((has_origin_prop, RDFS.label, Literal("has origin")))
        
        has_destination_prop = self.klm.hasDestination
        self.graph.add((has_destination_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((has_destination_prop, RDFS.label, Literal("has destination")))
        
        operates_prop = self.klm.operates
        self.graph.add((operates_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((operates_prop, RDFS.label, Literal("operates")))
        
        follows_prop = self.klm.follows
        self.graph.add((follows_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((follows_prop, RDFS.label, Literal("follows")))
    
    def get_or_create_airport(self, code: str, name: str = None, **kwargs) -> URIRef:
        """Get or create airport entity with deduplication"""
        if not code or pd.isna(code):
            return None
            
        code = str(code).upper()
        
        if code in self.airports:
            airport_uri = self.airports[code]
            # Update with additional info if provided
            if name and not pd.isna(name):
                self.graph.add((airport_uri, self.klm.name, Literal(str(name))))
                self.graph.add((airport_uri, self.schema.name, Literal(str(name))))
        else:
            airport_uri = self.klm[f"airport/{code}"]
            self.airports[code] = airport_uri
            
            # Basic airport info
            self.graph.add((airport_uri, RDF.type, self.klm.Airport))
            self.graph.add((airport_uri, self.klm.code, Literal(code)))
            
            if name and not pd.isna(name):
                self.graph.add((airport_uri, self.klm.name, Literal(str(name))))
                self.graph.add((airport_uri, self.schema.name, Literal(str(name))))
            
            # Initialize metrics tracking
            self.airport_metrics[code] = {
                'routes': set(),
                'flights': 0,
                'delayed_flights': 0,
                'total_delay_minutes': 0,
                'airlines': set()
            }
        
        # Add optional properties
        for prop, value in kwargs.items():
            if value is not None and not pd.isna(value):
                if prop in ['latitude', 'lat']:
                    self.graph.add((airport_uri, self.geo.lat, Literal(float(value), datatype=XSD.decimal)))
                elif prop in ['longitude', 'long', 'lng']:
                    self.graph.add((airport_uri, self.geo.long, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'passenger_volume':
                    self.graph.add((airport_uri, self.klm.passengerVolume, Literal(int(value), datatype=XSD.integer)))
        
        return airport_uri
    
    def get_or_create_airline(self, code: str, name: str = None, **kwargs) -> URIRef:
        """Get or create airline entity with deduplication"""
        if not code or pd.isna(code):
            return None
            
        code = str(code).upper()
        
        if code in self.airlines:
            airline_uri = self.airlines[code]
            if name and not pd.isna(name):
                self.graph.add((airline_uri, self.klm.name, Literal(str(name))))
        else:
            airline_uri = self.klm[f"airline/{code}"]
            self.airlines[code] = airline_uri
            
            self.graph.add((airline_uri, RDF.type, self.klm.Airline))
            self.graph.add((airline_uri, self.klm.code, Literal(code)))
            
            if name and not pd.isna(name):
                self.graph.add((airline_uri, self.klm.name, Literal(str(name))))
                self.graph.add((airline_uri, self.schema.name, Literal(str(name))))
            
            # Special handling for KLM
            if code == "KL":
                self.graph.add((airline_uri, self.klm.isCurrentProject, Literal(True, datatype=XSD.boolean)))
                # Mark Schiphol as KLM's main hub
                ams_uri = self.get_or_create_airport("AMS", "Amsterdam Airport Schiphol")
                self.graph.add((ams_uri, RDF.type, self.klm.HubAirport))
                self.graph.add((airline_uri, self.klm.hasHub, ams_uri))
                self.graph.add((ams_uri, self.klm.isMainHub, Literal(True, datatype=XSD.boolean)))
        
        return airline_uri
    
    def get_or_create_city_country(self, city_name: str, country_name: str = None) -> Tuple[URIRef, URIRef]:
        """Get or create city and country entities"""
        city_uri = country_uri = None
        
        if city_name and not pd.isna(city_name):
            city_slug = re.sub(r'[^a-z0-9]', '_', str(city_name).lower())
            
            if city_slug not in self.cities:
                city_uri = self.klm[f"city/{city_slug}"]
                self.cities[city_slug] = city_uri
                
                self.graph.add((city_uri, RDF.type, self.klm.City))
                self.graph.add((city_uri, self.klm.name, Literal(str(city_name))))
                self.graph.add((city_uri, self.schema.name, Literal(str(city_name))))
            else:
                city_uri = self.cities[city_slug]
        
        if country_name and not pd.isna(country_name):
            country_slug = re.sub(r'[^a-z0-9]', '_', str(country_name).lower())
            
            if country_slug not in self.countries:
                country_uri = self.klm[f"country/{country_slug}"]
                self.countries[country_slug] = country_uri
                
                self.graph.add((country_uri, RDF.type, self.klm.Country))
                self.graph.add((country_uri, self.klm.name, Literal(str(country_name))))
                self.graph.add((country_uri, self.schema.name, Literal(str(country_name))))
            else:
                country_uri = self.countries[country_slug]
            
            # Link city to country
            if city_uri and country_uri:
                self.graph.add((city_uri, self.klm.locatedIn, country_uri))
        
        return city_uri, country_uri
    
    def get_or_create_route(self, origin_code: str, dest_code: str, **kwargs) -> URIRef:
        """Get or create route entity"""
        if not origin_code or not dest_code or pd.isna(origin_code) or pd.isna(dest_code):
            return None
        
        origin_code = str(origin_code).upper()
        dest_code = str(dest_code).upper()
        route_key = f"{origin_code}-{dest_code}"
        
        if route_key not in self.routes:
            route_uri = self.klm[f"route/{route_key}"]
            self.routes[route_key] = route_uri
            
            self.graph.add((route_uri, RDF.type, self.klm.Route))
            
            # Link to airports
            origin_uri = self.get_or_create_airport(origin_code)
            dest_uri = self.get_or_create_airport(dest_code)
            
            if origin_uri:
                self.graph.add((route_uri, self.klm.hasOrigin, origin_uri))
            if dest_uri:
                self.graph.add((route_uri, self.klm.hasDestination, dest_uri))
            
            # Track route for metrics
            if origin_code in self.airport_metrics:
                self.airport_metrics[origin_code]['routes'].add(route_key)
            if dest_code in self.airport_metrics:
                self.airport_metrics[dest_code]['routes'].add(route_key)
            
            # Add route properties
            for prop, value in kwargs.items():
                if value is not None and not pd.isna(value):
                    if prop == 'duration':
                        self.graph.add((route_uri, self.klm.scheduledDuration, Literal(str(value), datatype=XSD.duration)))
                    elif prop == 'is_eu':
                        self.graph.add((route_uri, self.sch.isEU, Literal(bool(value), datatype=XSD.boolean)))
                    elif prop == 'requires_visa':
                        self.graph.add((route_uri, self.sch.requiresVisa, Literal(bool(value), datatype=XSD.boolean)))
        
        return self.routes[route_key]
    
    def process_unified_airports(self, data: Dict):
        """Process and unify airport data from both KLM and Schiphol sources"""
        logger.info("Processing unified airport data")
        
        airports_processed = set()
        
        # Process KLM airports
        if not data['klm_airports'].empty:
            for _, row in data['klm_airports'].iterrows():
                if pd.notna(row.get('airport_code')):
                    code = str(row['airport_code']).upper()
                    airports_processed.add(code)
                    
                    airport_uri = self.get_or_create_airport(
                        code=code,
                        name=row.get('airport_name'),
                        latitude=row.get('latitude'),
                        longitude=row.get('longitude')
                    )
                    
                    # Add geographic relationships
                    if pd.notna(row.get('city')) or pd.notna(row.get('country')):
                        city_uri, country_uri = self.get_or_create_city_country(
                            row.get('city'), row.get('country')
                        )
                        if city_uri:
                            self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
        
        # Process Schiphol destinations (additional airports)
        if not data['sch_destinations'].empty:
            for _, row in data['sch_destinations'].iterrows():
                if pd.notna(row.get('iata')):
                    code = str(row['iata']).upper()
                    
                    if code not in airports_processed:
                        airports_processed.add(code)
                        
                        airport_uri = self.get_or_create_airport(
                            code=code,
                            name=row.get('name_english') or row.get('city')
                        )
                        
                        # Add geographic relationships
                        if pd.notna(row.get('city')) or pd.notna(row.get('country')):
                            city_uri, country_uri = self.get_or_create_city_country(
                                row.get('city'), row.get('country')
                            )
                            if city_uri:
                                self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
        
        # Add Schiphol as main hub if not already processed
        if "AMS" not in airports_processed:
            ams_uri = self.get_or_create_airport("AMS", "Amsterdam Airport Schiphol")
            self.graph.add((ams_uri, RDF.type, self.klm.HubAirport))
            self.graph.add((ams_uri, self.sch.isMainSchipholHub, Literal(True, datatype=XSD.boolean)))
        
        logger.info(f"Processed {len(airports_processed)} unique airports")
    
    def process_unified_airlines(self, data: Dict):
        """Process and unify airline data from both sources"""
        logger.info("Processing unified airline data")
        
        airlines_processed = set()
        
        # Process KLM airlines
        if not data['klm_airlines'].empty:
            for _, row in data['klm_airlines'].iterrows():
                if pd.notna(row.get('airline_code')):
                    code = str(row['airline_code']).upper()
                    airlines_processed.add(code)
                    
                    self.get_or_create_airline(
                        code=code,
                        name=row.get('airline_name')
                    )
        
        # Process Schiphol airlines
        if not data['sch_airlines'].empty:
            for _, row in data['sch_airlines'].iterrows():
                if pd.notna(row.get('iata')):
                    code = str(row['iata']).upper()
                    
                    if code not in airlines_processed:
                        airlines_processed.add(code)
                        
                        airline_uri = self.get_or_create_airline(
                            code=code,
                            name=row.get('public_name')
                        )
                        
                        # Add ICAO code if available
                        if pd.notna(row.get('icao')):
                            self.graph.add((airline_uri, self.sch.icaoCode, Literal(str(row['icao']))))
        
        logger.info(f"Processed {len(airlines_processed)} unique airlines")
    
    def process_unified_flights_and_routes(self, data: Dict):
        """Process flights and routes from both sources, building comprehensive route network"""
        logger.info("Processing unified flights and routes")
        
        flights_processed = 0
        routes_processed = set()
        delay_data = {}
        
        # Process KLM flights and routes
        if not data['klm_flights'].empty:
            for _, row in data['klm_flights'].iterrows():
                flights_processed += 1
                
                # Create route if origin/destination available
                if pd.notna(row.get('departure_airport_code')) and pd.notna(row.get('arrival_airport_code')):
                    origin = str(row['departure_airport_code']).upper()
                    destination = str(row['arrival_airport_code']).upper()
                    route_key = f"{origin}-{destination}"
                    
                    if route_key not in routes_processed:
                        routes_processed.add(route_key)
                        route_uri = self.get_or_create_route(
                            origin, destination,
                            duration=row.get('scheduled_duration')
                        )
                        
                        # Link airline to route
                        if pd.notna(row.get('airline_code')):
                            airline_uri = self.get_or_create_airline(row['airline_code'], row.get('airline_name'))
                            if airline_uri and route_uri:
                                self.graph.add((airline_uri, self.klm.operates, route_uri))
                    
                    # Track delays for destination
                    if destination not in delay_data:
                        delay_data[destination] = {'total': 0, 'delayed': 0, 'delay_minutes': 0}
                    
                    delay_data[destination]['total'] += 1
                    
                    # Check for delays
                    is_delayed = False
                    if pd.notna(row.get('scheduled_arrival_time')) and pd.notna(row.get('estimated_arrival_time')):
                        if str(row['estimated_arrival_time']) > str(row['scheduled_arrival_time']):
                            is_delayed = True
                            delay_data[destination]['delayed'] += 1
                    
                    # Update airport metrics
                    if destination in self.airport_metrics:
                        self.airport_metrics[destination]['flights'] += 1
                        if is_delayed:
                            self.airport_metrics[destination]['delayed_flights'] += 1
        
        # Process KLM routes (if separate from flights)
        if not data['klm_routes'].empty:
            for _, row in data['klm_routes'].iterrows():
                if pd.notna(row.get('origin')) and pd.notna(row.get('destination')):
                    origin = str(row['origin']).upper()
                    destination = str(row['destination']).upper()
                    route_key = f"{origin}-{destination}"
                    
                    if route_key not in routes_processed:
                        routes_processed.add(route_key)
                        route_uri = self.get_or_create_route(
                            origin, destination,
                            duration=row.get('scheduled_duration')
                        )
                        
                        if pd.notna(row.get('airline_code')):
                            airline_uri = self.get_or_create_airline(row['airline_code'])
                            if airline_uri and route_uri:
                                self.graph.add((airline_uri, self.klm.operates, route_uri))
        
        # Process Schiphol flights
        schiphol_flights = data['sch_flights_enriched'] if not data['sch_flights_enriched'].empty else data['sch_flights']
        
        if not schiphol_flights.empty:
            for _, row in schiphol_flights.iterrows():
                if pd.notna(row.get('destinations')):
                    destinations = str(row['destinations']).split(', ')
                    if destinations:
                        destination = destinations[0].upper()
                        route_key = f"AMS-{destination}"
                        
                        if route_key not in routes_processed:
                            routes_processed.add(route_key)
                            route_uri = self.get_or_create_route(
                                "AMS", destination,
                                is_eu=row.get('eu') == 'Y' if pd.notna(row.get('eu')) else None,
                                requires_visa=row.get('visa_required') == 'Y' if pd.notna(row.get('visa_required')) else None
                            )
                        
                        # Track airline operations
                        if pd.notna(row.get('airline_code')):
                            airline_uri = self.get_or_create_airline(row['airline_code'])
                            route_uri = self.routes.get(route_key)
                            if airline_uri and route_uri:
                                self.graph.add((airline_uri, self.klm.operates, route_uri))
                        
                        # Track Schiphol-specific delays
                        if destination not in delay_data:
                            delay_data[destination] = {'total': 0, 'delayed': 0, 'delay_minutes': 0}
                        
                        delay_data[destination]['total'] += 1
                        
                        # Check flight states for delays
                        if pd.notna(row.get('flight_states')) and 'delayed' in str(row['flight_states']).lower():
                            delay_data[destination]['delayed'] += 1
                            if destination in self.airport_metrics:
                                self.airport_metrics[destination]['delayed_flights'] += 1
                        
                        if destination in self.airport_metrics:
                            self.airport_metrics[destination]['flights'] += 1
        
        # Add delay statistics to airports
        for airport_code, stats in delay_data.items():
            if stats['total'] > 0:
                airport_uri = self.airports.get(airport_code)
                if airport_uri:
                    delay_rate = stats['delayed'] / stats['total']
                    self.graph.add((airport_uri, self.klm.delayRate, Literal(delay_rate, datatype=XSD.decimal)))
                    self.graph.add((airport_uri, self.klm.totalFlights, Literal(stats['total'], datatype=XSD.integer)))
                    self.graph.add((airport_uri, self.klm.delayedFlights, Literal(stats['delayed'], datatype=XSD.integer)))
        
        logger.info(f"Processed {flights_processed} flights and {len(routes_processed)} unique routes")
    
    def process_aircraft_types(self, data: Dict):
        """Process aircraft type data from both sources"""
        logger.info("Processing aircraft types")
        
        aircraft_processed = set()
        
        # Process Schiphol aircraft types
        if not data['sch_aircraft_types'].empty:
            for _, row in data['sch_aircraft_types'].iterrows():
                if pd.notna(row.get('iata_main')):
                    code = str(row['iata_main']).upper()
                    
                    if code not in aircraft_processed:
                        aircraft_processed.add(code)
                        aircraft_uri = self.klm[f"aircraft/{code}"]
                        self.aircraft[code] = aircraft_uri
                        
                        self.graph.add((aircraft_uri, RDF.type, self.klm.Aircraft))
                        self.graph.add((aircraft_uri, self.klm.code, Literal(code)))
                        
                        if pd.notna(row.get('long_description')):
                            self.graph.add((aircraft_uri, self.klm.name, Literal(str(row['long_description']))))
                        elif pd.notna(row.get('short_description')):
                            self.graph.add((aircraft_uri, self.klm.name, Literal(str(row['short_description']))))
                        
                        # Add capacity estimation based on aircraft type
                        capacity = self._estimate_aircraft_capacity(code)
                        if capacity > 0:
                            self.graph.add((aircraft_uri, self.sch.capacity, Literal(capacity, datatype=XSD.integer)))
        
        logger.info(f"Processed {len(aircraft_processed)} aircraft types")
    
    def _estimate_aircraft_capacity(self, aircraft_code: str) -> int:
        """Estimate aircraft capacity based on aircraft type code"""
        aircraft_code = aircraft_code.upper()
        
        # Common aircraft capacity estimates
        capacity_map = {
            '747': 400, '777': 300, '737': 150, '320': 180, '321': 220,
            '319': 150, '330': 250, '340': 280, 'A380': 500, 'A350': 300,
            'EMB': 100, 'CRJ': 80, 'DHC': 50, 'ATR': 70
        }
        
        for aircraft_type, capacity in capacity_map.items():
            if aircraft_type in aircraft_code:
                return capacity
        
        return 150  # Default capacity
    
    def calculate_unified_hub_metrics(self):
        """Calculate comprehensive hub metrics using data from both sources"""
        logger.info("Calculating unified hub metrics")
        
        # Calculate route counts and connectivity scores
        for airport_code, metrics in self.airport_metrics.items():
            airport_uri = self.airports.get(airport_code)
            if not airport_uri:
                continue
            
            # Basic route count
            route_count = len(metrics['routes'])
            self.graph.add((airport_uri, self.klm.routeCount, Literal(route_count, datatype=XSD.integer)))
            
            # Calculate connectivity index (routes * average flights per route)
            if route_count > 0 and metrics['flights'] > 0:
                connectivity_index = route_count * (metrics['flights'] / route_count)
                self.graph.add((airport_uri, self.klm.connectivityIndex, Literal(connectivity_index, datatype=XSD.decimal)))
            
            # Calculate hub potential score
            hub_score = self._calculate_hub_score(airport_code, metrics, route_count)
            if hub_score > 0:
                self.graph.add((airport_uri, self.klm.hubPotentialScore, Literal(hub_score, datatype=XSD.decimal)))
                
                # Mark as potential hub if score is high enough
                if hub_score > 10 and airport_code != "AMS":  # Exclude current hub
                    self.graph.add((airport_uri, RDF.type, self.klm.PotentialHubAirport))
        
        logger.info("Hub metrics calculation completed")
    
    def _calculate_hub_score(self, airport_code: str, metrics: Dict, route_count: int) -> float:
        """Calculate comprehensive hub potential score"""
        if route_count == 0:
            return 0.0
        
        # Base score from route connectivity
        base_score = route_count * 2
        
        # Adjust for delay performance (lower delays = higher score)
        if metrics['flights'] > 0:
            delay_rate = metrics['delayed_flights'] / metrics['flights']
            delay_factor = max(0.5, 1.0 - delay_rate)  # Between 0.5 and 1.0
            base_score *= delay_factor
        
        # Check for passenger volume boost
        passenger_query = f"""
            SELECT ?passengerVolume
            WHERE {{
                <{self.klm}airport/{airport_code}> klm:passengerVolume ?passengerVolume .
            }}
        """
        
        try:
            results = list(self.graph.query(passenger_query))
            if results:
                passenger_volume = float(results[0].passengerVolume)
                # Normalize passenger volume (10M passengers = 1.0 multiplier)
                passenger_factor = min(2.0, passenger_volume / 10000000)
                base_score *= passenger_factor
        except:
            pass
        
        # EU location bonus
        eu_query = f"""
            SELECT ?isEU
            WHERE {{
                ?route klm:hasDestination <{self.klm}airport/{airport_code}> .
                ?route sch:isEU ?isEU .
                FILTER(?isEU = true)
            }}
        """
        
        try:
            eu_results = list(self.graph.query(eu_query))
            if eu_results:
                base_score *= 1.2  # 20% bonus for EU airports
        except:
            pass
        
        return base_score
    
    def add_eurostat_passenger_data(self):
        """Add realistic passenger volume data for major European airports"""
        logger.info("Adding passenger volume data")
        
        # Major European airports with annual passenger volumes (millions)
        passenger_data = {
            'AMS': 71, 'CDG': 76, 'FRA': 70, 'LHR': 80, 'MAD': 61,
            'FCO': 48, 'MUC': 47, 'BCN': 52, 'LGW': 46, 'ORY': 32,
            'BRU': 26, 'DUB': 31, 'MAN': 29, 'VIE': 31, 'CPH': 30,
            'HEL': 21, 'ZRH': 31, 'OSL': 28, 'ARN': 27, 'LIS': 31,
            'WAW': 18, 'PRG': 17, 'BUD': 16, 'ATH': 25, 'IST': 68
        }
        
        for airport_code, volume_millions in passenger_data.items():
            airport_uri = self.get_or_create_airport(airport_code)
            if airport_uri:
                passenger_count = volume_millions * 1000000
                self.graph.add((airport_uri, self.klm.passengerVolume, 
                              Literal(passenger_count, datatype=XSD.integer)))
        
        logger.info(f"Added passenger data for {len(passenger_data)} airports")
    
    def generate_comprehensive_queries(self):
        """Generate SPARQL queries for comprehensive hub expansion analysis"""
        logger.info("Generating comprehensive SPARQL queries")
        
        queries = {}
        
        # Primary research question: Optimal second hub selection
        queries["optimal_second_hub"] = """
            # Comprehensive second hub analysis
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country 
                   ?routeCount ?passengerVolume ?hubScore 
                   ?connectivityIndex ?delayRate ?isEU
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:routeCount ?routeCount ;
                        klm:hubPotentialScore ?hubScore .
                
                # Exclude current main hub
                FILTER NOT EXISTS { ?airport klm:isMainHub true }
                
                # Minimum thresholds for hub consideration
                FILTER(?routeCount >= 8)
                
                OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
                OPTIONAL { ?airport klm:connectivityIndex ?connectivityIndex }
                OPTIONAL { ?airport klm:delayRate ?delayRate }
                
                # EU status check
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:isEU ?isEU .
                }
                
                # Country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?hubScore) DESC(?passengerVolume)
            LIMIT 10
        """
        
        # Route expansion opportunities
        queries["expansion_opportunities"] = """
            # Route expansion analysis
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            
            SELECT ?airport ?name ?country ?passengerVolume ?isEU ?requiresVisa
                   (GROUP_CONCAT(DISTINCT ?competitor; SEPARATOR=', ') AS ?competitors)
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name .
                
                # High passenger volume destinations
                ?airport klm:passengerVolume ?passengerVolume .
                FILTER(?passengerVolume > 15000000)
                
                # Not currently served by KLM from AMS
                FILTER NOT EXISTS {
                    ?klmRoute klm:hasOrigin <http://example.org/klm/airport/AMS> ;
                             klm:hasDestination ?airport .
                    ?klmAirline klm:operates ?klmRoute ;
                               klm:code "KL" .
                }
                
                # But served by other airlines (market validation)
                ?competitorRoute klm:hasOrigin <http://example.org/klm/airport/AMS> ;
                                klm:hasDestination ?airport .
                ?competitorAirline klm:operates ?competitorRoute ;
                                  klm:name ?competitor .
                FILTER(?competitorAirline != <http://example.org/klm/airline/KL>)
                
                OPTIONAL { 
                    ?competitorRoute sch:isEU ?isEU .
                    ?competitorRoute sch:requiresVisa ?requiresVisa .
                }
                
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            GROUP BY ?airport ?name ?country ?passengerVolume ?isEU ?requiresVisa
            ORDER BY DESC(?passengerVolume)
            LIMIT 15
        """
        
        # Delay correlation analysis
        queries["delay_correlation_analysis"] = """
            # Delay analysis by destination
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            
            SELECT ?airport ?name ?country ?delayRate ?totalFlights ?delayedFlights 
                   ?isEU ?routeCount ?passengerVolume
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:delayRate ?delayRate ;
                        klm:totalFlights ?totalFlights ;
                        klm:delayedFlights ?delayedFlights .
                
                # Minimum flight volume for statistical significance
                FILTER(?totalFlights >= 10)
                
                OPTIONAL { ?airport klm:routeCount ?routeCount }
                OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
                
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:isEU ?isEU .
                }
                
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?delayRate) DESC(?totalFlights)
        """
        
        # Competitive analysis
        queries["competitive_landscape"] = """
            # Competitive landscape analysis
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            
            SELECT ?airport ?name ?country 
                   (COUNT(DISTINCT ?airline) AS ?airlineCount)
                   (COUNT(DISTINCT ?route) AS ?routeCount)
                   ?passengerVolume ?hubScore
            WHERE {
                ?route klm:hasDestination ?airport .
                ?airline klm:operates ?route ;
                        klm:name ?airlineName .
                
                ?airport klm:name ?name .
                
                OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
                OPTIONAL { ?airport klm:hubPotentialScore ?hubScore }
                
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            GROUP BY ?airport ?name ?country ?passengerVolume ?hubScore
            HAVING (?airlineCount > 3)
            ORDER BY DESC(?airlineCount) DESC(?routeCount)
            LIMIT 20
        """
        
        # Save queries to files
        queries_dir = os.path.join(self.output_dir, "queries")
        os.makedirs(queries_dir, exist_ok=True)
        
        for name, query in queries.items():
            query_path = os.path.join(queries_dir, f"{name}.sparql")
            with open(query_path, 'w', encoding='utf-8') as f:
                f.write(query)
            logger.info(f"Saved query '{name}' to {query_path}")
        
        return queries
    
    def build_unified_knowledge_graph(self):
        """Build the complete unified knowledge graph"""
        logger.info("Building unified knowledge graph for KLM hub expansion analysis")
        
        # Load all data
        data = self.load_all_data()
        
        # Create unified ontology
        self.create_unified_ontology()
        
        # Process all entities
        self.process_unified_airports(data)
        self.process_unified_airlines(data)
        self.process_unified_flights_and_routes(data)
        self.process_aircraft_types(data)
        
        # Add external data
        self.add_eurostat_passenger_data()
        
        # Calculate comprehensive metrics
        self.calculate_unified_hub_metrics()
        
        # Generate research queries
        self.generate_comprehensive_queries()
        
        # Save the unified knowledge graph
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(self.output_dir, f"unified_klm_hub_expansion_kg_{timestamp}.ttl")
        
        self.graph.serialize(destination=output_path, format="turtle")
        logger.info(f"Unified knowledge graph saved to {output_path}")
        
        # Also save as RDF/XML
        xml_output_path = os.path.join(self.output_dir, f"unified_klm_hub_expansion_kg_{timestamp}.rdf")
        self.graph.serialize(destination=xml_output_path, format="xml")
        logger.info(f"Unified knowledge graph also saved as RDF/XML to {xml_output_path}")
        
        # Generate summary statistics
        stats = {
            'total_triples': len(self.graph),
            'airports': len(self.airports),
            'airlines': len(self.airlines),
            'routes': len(self.routes),
            'cities': len(self.cities),
            'countries': len(self.countries),
            'aircraft_types': len(self.aircraft),
            'output_file': output_path
        }
        
        logger.info(f"Knowledge graph build complete:")
        logger.info(f"  - {stats['total_triples']} total triples")
        logger.info(f"  - {stats['airports']} airports")
        logger.info(f"  - {stats['airlines']} airlines") 
        logger.info(f"  - {stats['routes']} routes")
        logger.info(f"  - {stats['cities']} cities")
        logger.info(f"  - {stats['countries']} countries")
        logger.info(f"  - {stats['aircraft_types']} aircraft types")
        
        return stats

def main():
    """Main function to build the unified knowledge graph"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build unified KLM-Schiphol knowledge graph")
    parser.add_argument("--klm-dir", default="data/KLM/processed", 
                        help="Directory containing processed KLM data")
    parser.add_argument("--schiphol-dir", default="data/Schiphol/processed",
                        help="Directory containing processed Schiphol data")
    parser.add_argument("--output-dir", default="data/knowledge_graph",
                        help="Directory to save the unified knowledge graph")
    
    args = parser.parse_args()
    
    # Create unified knowledge graph builder
    builder = UnifiedKnowledgeGraphBuilder(
        klm_processed_dir=args.klm_dir,
        schiphol_processed_dir=args.schiphol_dir,
        output_dir=args.output_dir
    )
    
    # Build the unified knowledge graph
    try:
        stats = builder.build_unified_knowledge_graph()
        
        logger.info("Unified knowledge graph successfully created!")
        logger.info("The graph can now be loaded into a triplestore for querying and analysis.")
        logger.info("Use the generated SPARQL queries to answer KLM's hub expansion research questions.")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error building unified knowledge graph: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())