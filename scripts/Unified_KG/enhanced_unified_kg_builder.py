"""
Enhanced Unified KLM-Schiphol Knowledge Graph Builder

This script creates a unified knowledge graph integrating KLM, Schiphol, and ArcGIS Hub
airport data for comprehensive hub expansion analysis.
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
        logging.FileHandler("enhanced_unified_kg_builder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EnhancedUnifiedKGBuilder")

class EnhancedUnifiedKnowledgeGraphBuilder:
    """Enhanced Unified Knowledge Graph Builder for KLM Hub Expansion Analysis"""
    
    def __init__(self, klm_processed_dir='data/KLM/processed', 
                 schiphol_processed_dir='data/Schiphol/processed',
                 arcgis_processed_dir='data/ArcGIS_Hub',
                 output_dir='data/knowledge_graph'):
        """Initialize the enhanced unified knowledge graph builder"""
        self.klm_processed_dir = klm_processed_dir
        self.schiphol_processed_dir = schiphol_processed_dir
        self.arcgis_processed_dir = arcgis_processed_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize single RDF graph
        self.graph = Graph()
        
        # Define namespaces
        self.klm = Namespace("http://example.org/klm/")
        self.sch = Namespace("http://example.org/schiphol/")
        self.arcgis = Namespace("http://example.org/arcgis/")
        self.schema = Namespace("http://schema.org/")
        self.geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        self.dbo = Namespace("http://dbpedia.org/ontology/")
        
        # Bind namespaces
        self.graph.bind("klm", self.klm)
        self.graph.bind("sch", self.sch)
        self.graph.bind("arcgis", self.arcgis)
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
        
        logger.info("Enhanced Unified Knowledge Graph Builder initialized")
    
    def load_all_data(self) -> Dict:
        """Load all processed data from KLM, Schiphol, and ArcGIS sources"""
        logger.info("Loading all processed data from multiple sources")
        
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
        
        # Load ArcGIS Hub processed data
        arcgis_files = {
            'arcgis_all_airports': 'airports_processed.csv',
            'arcgis_hub_candidates': 'hub_candidates.csv'
        }
        
        for key, filename in arcgis_files.items():
            filepath = os.path.join(self.arcgis_processed_dir, filename)
            if os.path.exists(filepath):
                data[key] = pd.read_csv(filepath)
                logger.info(f"Loaded {len(data[key])} records from {filename}")
            else:
                logger.warning(f"ArcGIS file not found: {filepath}")
                data[key] = pd.DataFrame()
        
        return data
    
    def create_enhanced_ontology(self):
        """Create enhanced ontology incorporating ArcGIS infrastructure concepts"""
        logger.info("Creating enhanced ontology with ArcGIS concepts")
        
        # Core Classes (same as before)
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
        
        # Enhanced airport classes from ArcGIS data
        large_airport_class = self.arcgis.LargeAirport
        self.graph.add((large_airport_class, RDF.type, OWL.Class))
        self.graph.add((large_airport_class, RDFS.subClassOf, airport_class))
        self.graph.add((large_airport_class, RDFS.label, Literal("Large Airport")))
        
        medium_airport_class = self.arcgis.MediumAirport
        self.graph.add((medium_airport_class, RDF.type, OWL.Class))
        self.graph.add((medium_airport_class, RDFS.subClassOf, airport_class))
        self.graph.add((medium_airport_class, RDFS.label, Literal("Medium Airport")))
        
        # Infrastructure classes
        runway_class = self.arcgis.Runway
        self.graph.add((runway_class, RDF.type, OWL.Class))
        self.graph.add((runway_class, RDFS.label, Literal("Runway")))
        
        # Other classes (airlines, routes, flights, etc.)
        airline_class = self.klm.Airline
        self.graph.add((airline_class, RDF.type, OWL.Class))
        self.graph.add((airline_class, RDFS.label, Literal("Airline")))
        
        route_class = self.klm.Route
        self.graph.add((route_class, RDF.type, OWL.Class))
        self.graph.add((route_class, RDFS.label, Literal("Route")))
        
        flight_class = self.klm.Flight
        self.graph.add((flight_class, RDF.type, OWL.Class))
        self.graph.add((flight_class, RDFS.label, Literal("Flight")))
        
        # Geographic classes
        city_class = self.klm.City
        self.graph.add((city_class, RDF.type, OWL.Class))
        self.graph.add((city_class, RDFS.label, Literal("City")))
        
        country_class = self.klm.Country
        self.graph.add((country_class, RDF.type, OWL.Class))
        self.graph.add((country_class, RDFS.label, Literal("Country")))
        
        # Define enhanced properties
        self._define_enhanced_properties()
        
        logger.info("Enhanced ontology created")
    
    def _define_enhanced_properties(self):
        """Define enhanced data and object properties including ArcGIS concepts"""
        
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
        
        elevation_prop = self.arcgis.elevation
        self.graph.add((elevation_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((elevation_prop, RDFS.label, Literal("elevation")))
        
        # Infrastructure properties from ArcGIS
        runway_length_prop = self.arcgis.runwayLength
        self.graph.add((runway_length_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((runway_length_prop, RDFS.label, Literal("runway length")))
        
        runway_width_prop = self.arcgis.runwayWidth
        self.graph.add((runway_width_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((runway_width_prop, RDFS.label, Literal("runway width")))
        
        runway_capacity_prop = self.arcgis.runwayCapacity
        self.graph.add((runway_capacity_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((runway_capacity_prop, RDFS.label, Literal("runway capacity")))
        
        # Strategic analysis properties
        hub_priority_score_prop = self.arcgis.hubPriorityScore
        self.graph.add((hub_priority_score_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((hub_priority_score_prop, RDFS.label, Literal("hub priority score")))
        
        distance_from_ams_prop = self.arcgis.distanceFromAMS
        self.graph.add((distance_from_ams_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((distance_from_ams_prop, RDFS.label, Literal("distance from Amsterdam")))
        
        strategic_distance_prop = self.arcgis.strategicDistance
        self.graph.add((strategic_distance_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((strategic_distance_prop, RDFS.label, Literal("strategic distance category")))
        
        # Hub analysis properties (existing)
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
    
    def get_or_create_enhanced_airport(self, code: str, name: str = None, **kwargs) -> URIRef:
        """Enhanced airport creation with ArcGIS infrastructure data"""
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
        
        # Add enhanced properties from ArcGIS data
        for prop, value in kwargs.items():
            if value is not None and not pd.isna(value):
                if prop in ['latitude', 'lat', 'latitude_deg']:
                    self.graph.add((airport_uri, self.geo.lat, Literal(float(value), datatype=XSD.decimal)))
                elif prop in ['longitude', 'long', 'lng', 'longitude_deg']:
                    self.graph.add((airport_uri, self.geo.long, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'passenger_volume':
                    self.graph.add((airport_uri, self.klm.passengerVolume, Literal(int(value), datatype=XSD.integer)))
                elif prop == 'elevation_ft':
                    self.graph.add((airport_uri, self.arcgis.elevation, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'runway_length_m':
                    self.graph.add((airport_uri, self.arcgis.runwayLength, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'runway_width_m':
                    self.graph.add((airport_uri, self.arcgis.runwayWidth, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'runway_capacity':
                    self.graph.add((airport_uri, self.arcgis.runwayCapacity, Literal(str(value))))
                elif prop == 'hub_priority_score':
                    self.graph.add((airport_uri, self.arcgis.hubPriorityScore, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'distance_from_ams_km':
                    self.graph.add((airport_uri, self.arcgis.distanceFromAMS, Literal(float(value), datatype=XSD.decimal)))
                elif prop == 'strategic_distance':
                    self.graph.add((airport_uri, self.arcgis.strategicDistance, Literal(str(value))))
                elif prop == 'airport_type':
                    # Add specialized airport type classes
                    if str(value).lower() == 'large_airport':
                        self.graph.add((airport_uri, RDF.type, self.arcgis.LargeAirport))
                    elif str(value).lower() == 'medium_airport':
                        self.graph.add((airport_uri, RDF.type, self.arcgis.MediumAirport))
        
        return airport_uri
    
    def get_or_create_airline(self, code: str, name: str = None, **kwargs) -> URIRef:
        """Get or create airline entity with deduplication (same as before)"""
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
                ams_uri = self.get_or_create_enhanced_airport("AMS", "Amsterdam Airport Schiphol")
                self.graph.add((ams_uri, RDF.type, self.klm.HubAirport))
                self.graph.add((airline_uri, self.klm.hasHub, ams_uri))
                self.graph.add((ams_uri, self.klm.isMainHub, Literal(True, datatype=XSD.boolean)))
        
        return airline_uri
    
    def get_or_create_city_country(self, city_name: str, country_name: str = None, country_code: str = None) -> Tuple[URIRef, URIRef]:
        """Enhanced city/country creation with country codes"""
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
                
                # Add country code if available
                if country_code and not pd.isna(country_code):
                    self.graph.add((country_uri, self.arcgis.countryCode, Literal(str(country_code))))
                    
            else:
                country_uri = self.countries[country_slug]
            
            # Link city to country
            if city_uri and country_uri:
                self.graph.add((city_uri, self.klm.locatedIn, country_uri))
        
        return city_uri, country_uri
    
    def get_or_create_route(self, origin_code: str, dest_code: str, **kwargs) -> URIRef:
        """Get or create route entity (same as before with minor enhancements)"""
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
            origin_uri = self.get_or_create_enhanced_airport(origin_code)
            dest_uri = self.get_or_create_enhanced_airport(dest_code)
            
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
    
    def process_arcgis_airports(self, data: Dict):
        """Process ArcGIS Hub airport data for comprehensive coverage"""
        logger.info("Processing ArcGIS Hub airport data")
        
        arcgis_airports_added = 0
        
        # Process hub candidates first (highest priority)
        if not data['arcgis_hub_candidates'].empty:
            logger.info("Processing ArcGIS hub candidates")
            for _, row in data['arcgis_hub_candidates'].iterrows():
                if pd.notna(row.get('iata_code')):
                    code = str(row['iata_code']).upper()
                    
                    airport_uri = self.get_or_create_enhanced_airport(
                        code=code,
                        name=row.get('standard_name') or row.get('name'),
                        latitude_deg=row.get('latitude_deg'),
                        longitude_deg=row.get('longitude_deg'),
                        elevation_ft=row.get('elevation_ft'),
                        runway_length_m=row.get('runway_length_m'),
                        runway_width_m=row.get('runway_width_m'),
                        runway_capacity=row.get('runway_capacity'),
                        hub_priority_score=row.get('hub_priority_score'),
                        distance_from_ams_km=row.get('distance_from_ams_km'),
                        strategic_distance=row.get('strategic_distance'),
                        airport_type=row.get('type')
                    )
                    
                    # Add geographic relationships with enhanced data
                    if pd.notna(row.get('municipality')) or pd.notna(row.get('continent_name')):
                        city_uri, country_uri = self.get_or_create_city_country(
                            row.get('municipality'),
                            row.get('continent_name'),
                            row.get('iso_country')
                        )
                        if city_uri:
                            self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
                    
                    # Mark high-priority hub candidates
                    if pd.notna(row.get('hub_priority_score')) and float(row['hub_priority_score']) >= 6:
                        self.graph.add((airport_uri, RDF.type, self.klm.PotentialHubAirport))
                    
                    arcgis_airports_added += 1
        
        # Process all airports to fill gaps in coverage
        if not data['arcgis_all_airports'].empty:
            logger.info("Processing additional ArcGIS airports")
            for _, row in data['arcgis_all_airports'].iterrows():
                if pd.notna(row.get('iata_code')):
                    code = str(row['iata_code']).upper()
                    
                    # Only add if not already in the graph (avoid duplicates)
                    if code not in self.airports:
                        airport_uri = self.get_or_create_enhanced_airport(
                            code=code,
                            name=row.get('standard_name') or row.get('name'),
                            latitude_deg=row.get('latitude_deg'),
                            longitude_deg=row.get('longitude_deg'),
                            elevation_ft=row.get('elevation_ft'),
                            runway_length_m=row.get('runway_length_m'),
                            runway_width_m=row.get('runway_width_m'),
                            runway_capacity=row.get('runway_capacity'),
                            hub_priority_score=row.get('hub_priority_score'),
                            distance_from_ams_km=row.get('distance_from_ams_km'),
                            strategic_distance=row.get('strategic_distance'),
                            airport_type=row.get('type')
                        )
                        
                        # Add geographic relationships
                        if pd.notna(row.get('municipality')) or pd.notna(row.get('continent_name')):
                            city_uri, country_uri = self.get_or_create_city_country(
                                row.get('municipality'),
                                row.get('continent_name'),
                                row.get('iso_country')
                            )
                            if city_uri:
                                self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
                        
                        arcgis_airports_added += 1
        
        logger.info(f"Added {arcgis_airports_added} airports from ArcGIS Hub data")
    
    def process_unified_airports(self, data: Dict):
        """Process and unify airport data from all sources"""
        logger.info("Processing unified airport data from all sources")
        
        airports_processed = set()
        
        # First, process ArcGIS data for comprehensive coverage
        self.process_arcgis_airports(data)
        airports_processed.update(self.airports.keys())
        
        # Process KLM airports (may enhance existing ArcGIS entries)
        if not data['klm_airports'].empty:
            for _, row in data['klm_airports'].iterrows():
                if pd.notna(row.get('airport_code')):
                    code = str(row['airport_code']).upper()
                    airports_processed.add(code)
                    
                    # Get or enhance existing airport
                    airport_uri = self.get_or_create_enhanced_airport(
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
                        
                        airport_uri = self.get_or_create_enhanced_airport(
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
        
        # Ensure Schiphol is properly marked as main hub
        if "AMS" not in airports_processed:
            ams_uri = self.get_or_create_enhanced_airport("AMS", "Amsterdam Airport Schiphol")
            self.graph.add((ams_uri, RDF.type, self.klm.HubAirport))
            self.graph.add((ams_uri, self.sch.isMainSchipholHub, Literal(True, datatype=XSD.boolean)))
        
        logger.info(f"Processed {len(airports_processed)} unique airports from all sources")
    
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
    
    def calculate_enhanced_hub_metrics(self):
        """Calculate enhanced hub metrics using data from all sources including ArcGIS"""
        logger.info("Calculating enhanced hub metrics with ArcGIS infrastructure data")
        
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
            
            # Calculate enhanced hub potential score
            hub_score = self._calculate_enhanced_hub_score(airport_code, metrics, route_count)
            if hub_score > 0:
                self.graph.add((airport_uri, self.klm.hubPotentialScore, Literal(hub_score, datatype=XSD.decimal)))
                
                # Mark as potential hub if score is high enough
                if hub_score > 15 and airport_code != "AMS":  # Higher threshold with enhanced scoring
                    self.graph.add((airport_uri, RDF.type, self.klm.PotentialHubAirport))
        
        logger.info("Enhanced hub metrics calculation completed")
    
    def _calculate_enhanced_hub_score(self, airport_code: str, metrics: Dict, route_count: int) -> float:
        """Calculate enhanced hub potential score including ArcGIS infrastructure data"""
        if route_count == 0:
            return 0.0
        
        # Base score from route connectivity
        base_score = route_count * 2
        
        # Adjust for delay performance (lower delays = higher score)
        if metrics['flights'] > 0:
            delay_rate = metrics['delayed_flights'] / metrics['flights']
            delay_factor = max(0.5, 1.0 - delay_rate)  # Between 0.5 and 1.0
            base_score *= delay_factor
        
        # Enhanced scoring with ArcGIS infrastructure data
        infrastructure_bonus = 1.0
        
        # Check for runway capacity (from ArcGIS data)
        runway_query = f"""
            SELECT ?runwayCapacity ?runwayLength ?hubPriorityScore ?strategicDistance
            WHERE {{
                <{self.klm}airport/{airport_code}> arcgis:runwayCapacity ?runwayCapacity .
                OPTIONAL {{ <{self.klm}airport/{airport_code}> arcgis:runwayLength ?runwayLength }}
                OPTIONAL {{ <{self.klm}airport/{airport_code}> arcgis:hubPriorityScore ?hubPriorityScore }}
                OPTIONAL {{ <{self.klm}airport/{airport_code}> arcgis:strategicDistance ?strategicDistance }}
            }}
        """
        
        try:
            results = list(self.graph.query(runway_query))
            if results:
                result = results[0]
                
                # Runway capacity bonus
                if result.runwayCapacity:
                    if str(result.runwayCapacity).lower() == 'large':
                        infrastructure_bonus *= 1.3  # 30% bonus for large runways
                    elif str(result.runwayCapacity).lower() == 'medium':
                        infrastructure_bonus *= 1.1  # 10% bonus for medium runways
                
                # Strategic distance factor
                if result.strategicDistance:
                    distance_cat = str(result.strategicDistance).lower()
                    if distance_cat == 'continental':
                        infrastructure_bonus *= 1.2  # 20% bonus for optimal continental distance
                    elif distance_cat == 'intercontinental':
                        infrastructure_bonus *= 1.15  # 15% bonus for intercontinental reach
                
                # ArcGIS hub priority score integration
                if result.hubPriorityScore:
                    priority_score = float(result.hubPriorityScore)
                    priority_factor = min(1.5, 1.0 + (priority_score / 10.0))  # Up to 50% bonus
                    infrastructure_bonus *= priority_factor
        except:
            pass  # Continue with base scoring if query fails
        
        base_score *= infrastructure_bonus
        
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
    
    def add_enhanced_passenger_data(self):
        """Add enhanced passenger volume data including ArcGIS-identified airports"""
        logger.info("Adding enhanced passenger volume data")
        
        # Extended European airports with annual passenger volumes (millions)
        passenger_data = {
            'AMS': 71, 'CDG': 76, 'FRA': 70, 'LHR': 80, 'MAD': 61,
            'FCO': 48, 'MUC': 47, 'BCN': 52, 'LGW': 46, 'ORY': 32,
            'BRU': 26, 'DUB': 31, 'MAN': 29, 'VIE': 31, 'CPH': 30,
            'HEL': 21, 'ZRH': 31, 'OSL': 28, 'ARN': 27, 'LIS': 31,
            'WAW': 18, 'PRG': 17, 'BUD': 16, 'ATH': 25, 'IST': 68,
            'SVO': 49, 'LED': 19, 'DME': 29, 'VKO': 24, 'SXF': 35,
            'TXL': 35, 'HAM': 17, 'DUS': 25, 'STR': 12, 'CGN': 12,
            'NUE': 4, 'LYS': 11, 'MRS': 10, 'NCE': 14, 'TLS': 9,
            'BOD': 7, 'NTE': 7, 'LIL': 2, 'STN': 28, 'LTN': 18,
            'BHX': 13, 'EDI': 14, 'GLA': 9, 'NCL': 5, 'LBA': 4
        }
        
        for airport_code, volume_millions in passenger_data.items():
            airport_uri = self.get_or_create_enhanced_airport(airport_code)
            if airport_uri:
                passenger_count = volume_millions * 1000000
                self.graph.add((airport_uri, self.klm.passengerVolume, 
                              Literal(passenger_count, datatype=XSD.integer)))
        
        logger.info(f"Added passenger data for {len(passenger_data)} airports")
    
    def generate_enhanced_queries(self):
        """Generate enhanced SPARQL queries leveraging ArcGIS infrastructure data"""
        logger.info("Generating enhanced SPARQL queries with ArcGIS data")
        
        queries = {}
        
        # Enhanced hub selection query with infrastructure metrics
        queries["enhanced_hub_selection"] = """
            # Enhanced second hub analysis with infrastructure data
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX arcgis: <http://example.org/arcgis/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country 
                   ?routeCount ?passengerVolume ?hubScore 
                   ?runwayCapacity ?strategicDistance ?hubPriorityScore
                   ?distanceFromAMS ?delayRate
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:routeCount ?routeCount ;
                        klm:hubPotentialScore ?hubScore .
                
                # Exclude current main hub
                FILTER NOT EXISTS { ?airport klm:isMainHub true }
                
                # Minimum thresholds for hub consideration
                FILTER(?routeCount >= 5)
                
                # ArcGIS infrastructure data
                OPTIONAL { ?airport arcgis:runwayCapacity ?runwayCapacity }
                OPTIONAL { ?airport arcgis:strategicDistance ?strategicDistance }
                OPTIONAL { ?airport arcgis:hubPriorityScore ?hubPriorityScore }
                OPTIONAL { ?airport arcgis:distanceFromAMS ?distanceFromAMS }
                
                # Standard metrics
                OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
                OPTIONAL { ?airport klm:delayRate ?delayRate }
                
                # Country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?hubScore) DESC(?hubPriorityScore)
            LIMIT 15
        """
        
        # Infrastructure capacity analysis
        queries["infrastructure_analysis"] = """
            # Airport infrastructure capacity analysis
            PREFIX klm: <http://example.org/klm/>
            PREFIX arcgis: <http://example.org/arcgis/>
            
            SELECT ?airport ?name ?country ?runwayCapacity ?runwayLength ?runwayWidth
                   ?elevation ?strategicDistance ?hubPriorityScore ?passengerVolume
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name .
                
                # Focus on airports with infrastructure data
                ?airport arcgis:runwayCapacity ?runwayCapacity .
                
                OPTIONAL { ?airport arcgis:runwayLength ?runwayLength }
                OPTIONAL { ?airport arcgis:runwayWidth ?runwayWidth }
                OPTIONAL { ?airport arcgis:elevation ?elevation }
                OPTIONAL { ?airport arcgis:strategicDistance ?strategicDistance }
                OPTIONAL { ?airport arcgis:hubPriorityScore ?hubPriorityScore }
                OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
                
                # Country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
                
                # Focus on large capacity airports
                FILTER(?runwayCapacity = "large")
            }
            ORDER BY DESC(?hubPriorityScore) DESC(?passengerVolume)
        """
        
        # Strategic distance analysis for hub placement
        queries["strategic_distance_analysis"] = """
            # Strategic distance analysis for optimal hub placement
            PREFIX klm: <http://example.org/klm/>
            PREFIX arcgis: <http://example.org/arcgis/>
            
            SELECT ?airport ?name ?country ?strategicDistance ?distanceFromAMS
                   ?hubScore ?routeCount ?runwayCapacity
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        arcgis:strategicDistance ?strategicDistance ;
                        arcgis:distanceFromAMS ?distanceFromAMS .
                
                OPTIONAL { ?airport klm:hubPotentialScore ?hubScore }
                OPTIONAL { ?airport klm:routeCount ?routeCount }
                OPTIONAL { ?airport arcgis:runwayCapacity ?runwayCapacity }
                
                # Country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
                
                # Focus on continental and intercontinental distances
                FILTER(?strategicDistance = "continental" || ?strategicDistance = "intercontinental")
            }
            ORDER BY ?strategicDistance DESC(?hubScore)
        """
        
        # Route expansion with infrastructure constraints
        queries["infrastructure_constrained_expansion"] = """
            # Route expansion considering infrastructure constraints
            PREFIX klm: <http://example.org/klm/>
            PREFIX arcgis: <http://example.org/arcgis/>
            PREFIX sch: <http://example.org/schiphol/>
            
            SELECT ?airport ?name ?country ?passengerVolume ?runwayCapacity
                   ?strategicDistance ?isEU ?requiresVisa
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:passengerVolume ?passengerVolume ;
                        arcgis:runwayCapacity ?runwayCapacity .
                
                # High passenger volume destinations with good infrastructure
                FILTER(?passengerVolume > 10000000)
                FILTER(?runwayCapacity = "large" || ?runwayCapacity = "medium")
                
                # Not currently served by KLM from AMS
                FILTER NOT EXISTS {
                    ?klmRoute klm:hasOrigin <http://example.org/klm/airport/AMS> ;
                             klm:hasDestination ?airport .
                    ?klmAirline klm:operates ?klmRoute ;
                               klm:code "KL" .
                }
                
                OPTIONAL { ?airport arcgis:strategicDistance ?strategicDistance }
                
                # Check EU status and visa requirements
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:isEU ?isEU .
                    ?route sch:requiresVisa ?requiresVisa .
                }
                
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?passengerVolume) ?runwayCapacity
            LIMIT 20
        """
        
        # Save queries to files
        queries_dir = os.path.join(self.output_dir, "queries")
        os.makedirs(queries_dir, exist_ok=True)
        
        for name, query in queries.items():
            query_path = os.path.join(queries_dir, f"{name}.sparql")
            with open(query_path, 'w', encoding='utf-8') as f:
                f.write(query)
            logger.info(f"Saved enhanced query '{name}' to {query_path}")
        
        return queries
    
    def build_enhanced_knowledge_graph(self):
        """Build the enhanced unified knowledge graph with ArcGIS data"""
        logger.info("Building enhanced unified knowledge graph with ArcGIS Hub data")
        
        # Load all data from all sources
        data = self.load_all_data()
        
        # Create enhanced ontology
        self.create_enhanced_ontology()
        
        # Process all entities with ArcGIS enhancements
        self.process_unified_airports(data)
        self.process_unified_airlines(data)
        self.process_unified_flights_and_routes(data)
        self.process_aircraft_types(data)
        
        # Add enhanced external data
        self.add_enhanced_passenger_data()
        
        # Calculate enhanced metrics
        self.calculate_enhanced_hub_metrics()
        
        # Generate enhanced research queries
        self.generate_enhanced_queries()
        
        # Save the enhanced unified knowledge graph
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(self.output_dir, f"enhanced_unified_klm_hub_kg_{timestamp}.ttl")
        
        self.graph.serialize(destination=output_path, format="turtle")
        logger.info(f"Enhanced unified knowledge graph saved to {output_path}")
        
        # Also save as RDF/XML
        xml_output_path = os.path.join(self.output_dir, f"enhanced_unified_klm_hub_kg_{timestamp}.rdf")
        self.graph.serialize(destination=xml_output_path, format="xml")
        logger.info(f"Enhanced unified knowledge graph also saved as RDF/XML to {xml_output_path}")
        
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
        
        logger.info(f"Enhanced knowledge graph build complete:")
        logger.info(f"  - {stats['total_triples']} total triples")
        logger.info(f"  - {stats['airports']} airports (enhanced with ArcGIS data)")
        logger.info(f"  - {stats['airlines']} airlines") 
        logger.info(f"  - {stats['routes']} routes")
        logger.info(f"  - {stats['cities']} cities")
        logger.info(f"  - {stats['countries']} countries")
        logger.info(f"  - {stats['aircraft_types']} aircraft types")
        
        # Print ArcGIS integration summary
        arcgis_enhanced_count = 0
        for airport_code in self.airports:
            # Check if airport has ArcGIS data
            airport_uri = self.airports[airport_code]
            has_runway_data = any(
                (s, p, o) for s, p, o in self.graph 
                if s == airport_uri and 'arcgis' in str(p)
            )
            if has_runway_data:
                arcgis_enhanced_count += 1
        
        logger.info(f"  - {arcgis_enhanced_count} airports enhanced with ArcGIS infrastructure data")
        
        return stats

def main():
    """Main function to build the enhanced unified knowledge graph"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build enhanced unified KLM-Schiphol-ArcGIS knowledge graph")
    parser.add_argument("--klm-dir", default="data/KLM/processed", 
                        help="Directory containing processed KLM data")
    parser.add_argument("--schiphol-dir", default="data/Schiphol/processed",
                        help="Directory containing processed Schiphol data")
    parser.add_argument("--arcgis-dir", default="data/ArcGIS_Hub",
                        help="Directory containing processed ArcGIS Hub data")
    parser.add_argument("--output-dir", default="data/knowledge_graph",
                        help="Directory to save the enhanced unified knowledge graph")
    
    args = parser.parse_args()
    
    # Check if ArcGIS data exists
    arcgis_candidates_file = os.path.join(args.arcgis_dir, 'hub_candidates.csv')
    if not os.path.exists(arcgis_candidates_file):
        logger.warning(f"ArcGIS hub candidates file not found: {arcgis_candidates_file}")
        logger.info("Please run the airport CSV processor first:")
        logger.info("python scripts/airport_csv_processor.py Airports28062017_189278238873247918.csv")
        
        # Ask if user wants to continue without ArcGIS data
        response = input("Continue without ArcGIS data? (y/n): ")
        if response.lower() != 'y':
            return 1
    
    # Create enhanced unified knowledge graph builder
    builder = EnhancedUnifiedKnowledgeGraphBuilder(
        klm_processed_dir=args.klm_dir,
        schiphol_processed_dir=args.schiphol_dir,
        arcgis_processed_dir=args.arcgis_dir,
        output_dir=args.output_dir
    )
    
    # Build the enhanced unified knowledge graph
    try:
        stats = builder.build_enhanced_knowledge_graph()
        
        logger.info("🎉 Enhanced unified knowledge graph successfully created!")
        logger.info("📊 Key enhancements from ArcGIS Hub data:")
        logger.info("   • Comprehensive airport infrastructure data (runways, capacity)")
        logger.info("   • Strategic distance analysis from Amsterdam")
        logger.info("   • Hub priority scoring based on multiple factors")
        logger.info("   • Enhanced geographic coverage of European airports")
        logger.info("")
        logger.info("🔍 The enhanced graph can now be used for:")
        logger.info("   • More accurate hub candidate identification")
        logger.info("   • Infrastructure-aware route planning")
        logger.info("   • Strategic distance-based hub placement")
        logger.info("   • Comprehensive European airport analysis")
        logger.info("")
        logger.info("📁 Use the enhanced SPARQL queries for advanced analysis!")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error building enhanced unified knowledge graph: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())