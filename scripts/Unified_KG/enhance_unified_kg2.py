"""
Fixed Enhanced Unified KLM-Schiphol Knowledge Graph Builder

This script fixes the airport deduplication issue in the unified knowledge graph
by implementing proper entity resolution and data consolidation.
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
import difflib

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
    """Enhanced Unified Knowledge Graph Builder for KLM Hub Expansion Analysis with Proper Deduplication"""
    
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
        
        # Enhanced deduplication registries
        self.airports: Dict[str, URIRef] = {}
        self.airport_data: Dict[str, Dict] = {}  # Consolidated airport data
        self.airlines: Dict[str, URIRef] = {}
        self.cities: Dict[str, URIRef] = {}
        self.countries: Dict[str, URIRef] = {}
        self.routes: Dict[str, URIRef] = {}
        self.aircraft: Dict[str, URIRef] = {}
        
        # Metrics tracking for analysis
        self.airport_metrics: Dict[str, Dict] = {}
        self.route_metrics: Dict[str, Dict] = {}
        
        # Name similarity threshold for deduplication
        self.name_similarity_threshold = 0.8
        
        logger.info("Enhanced Unified Knowledge Graph Builder initialized with proper deduplication")
    
    def normalize_airport_name(self, name):
        """Normalize airport name for better comparison and deduplication"""
        if pd.isna(name) or not name:
            return ""
        
        # Convert to lowercase and remove common variations
        normalized = str(name).lower().strip()
        
        # Remove common airport suffixes and prefixes
        suffixes_to_remove = [
            "international airport", "airport", "intl", "international",
            "regional", "field", "airfield", "airbase", "air base",
            "regional airport", "municipal airport", "county airport"
        ]
        
        for suffix in suffixes_to_remove:
            if normalized.endswith(f" {suffix}"):
                normalized = normalized[:-len(f" {suffix}")].strip()
        
        # Remove special characters and extra spaces
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def are_names_similar(self, name1, name2):
        """Check if two airport names are similar enough to be considered the same"""
        if not name1 or not name2:
            return False
        
        norm1 = self.normalize_airport_name(name1)
        norm2 = self.normalize_airport_name(name2)
        
        if norm1 == norm2:
            return True
        
        # Use sequence matcher for similarity
        similarity = difflib.SequenceMatcher(None, norm1, norm2).ratio()
        return similarity >= self.name_similarity_threshold
    
    def consolidate_airport_names(self, names_list):
        """Consolidate multiple airport names into the best representative name"""
        if not names_list:
            return ""
        
        # Filter out empty/null names
        valid_names = [str(name) for name in names_list if pd.notna(name) and str(name).strip()]
        
        if not valid_names:
            return ""
        
        if len(valid_names) == 1:
            return valid_names[0]
        
        # Prefer longer, more descriptive names that don't look like codes
        descriptive_names = [
            name for name in valid_names 
            if len(name) > 5 and not name.isupper() and not name.replace('-', '').replace(' ', '').isupper()
        ]
        
        if descriptive_names:
            # Sort by length and pick the longest descriptive name
            return max(descriptive_names, key=len)
        else:
            # Fall back to the longest name overall
            return max(valid_names, key=len)
    
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
    
    def consolidate_all_airport_data(self, data: Dict) -> Dict[str, Dict]:
        """Consolidate airport data from all sources to eliminate duplicates"""
        logger.info("Consolidating airport data from all sources to eliminate duplicates")
        
        consolidated_airports = {}
        
        # Define column mappings for each source
        source_mappings = {
            'arcgis_hub_candidates': {
                'code_col': 'iata_code',
                'name_col': 'standard_name',
                'alt_name_col': 'name',
                'city_col': 'municipality',
                'country_col': 'continent_name',
                'country_code_col': 'iso_country',
                'lat_col': 'latitude_deg',
                'lon_col': 'longitude_deg',
                'source': 'arcgis_hub_candidates'
            },
            'arcgis_all_airports': {
                'code_col': 'iata_code',
                'name_col': 'standard_name',
                'alt_name_col': 'name',
                'city_col': 'municipality',
                'country_col': 'continent_name',
                'country_code_col': 'iso_country',
                'lat_col': 'latitude_deg',
                'lon_col': 'longitude_deg',
                'source': 'arcgis_all_airports'
            },
            'klm_airports': {
                'code_col': 'airport_code',
                'name_col': 'airport_name',
                'city_col': 'city',
                'country_col': 'country',
                'lat_col': 'latitude',
                'lon_col': 'longitude',
                'source': 'klm_airports'
            },
            'sch_destinations': {
                'code_col': 'iata',
                'name_col': 'name_english',
                'alt_name_col': 'city',
                'city_col': 'city',
                'country_col': 'country',
                'lat_col': None,  # Schiphol destinations don't have coordinates
                'lon_col': None,  # Schiphol destinations don't have coordinates
                'source': 'sch_destinations'
            }
        }
        
        # Process each data source
        for source_key, mapping in source_mappings.items():
            if source_key in data and not data[source_key].empty:
                df = data[source_key]
                logger.info(f"Processing {len(df)} airports from {source_key}")
                
                for _, row in df.iterrows():
                    # Get airport code
                    code = row.get(mapping['code_col'])
                    if pd.isna(code) or not str(code).strip():
                        continue
                    
                    code = str(code).upper().strip()
                    
                    # Initialize airport data if not exists
                    if code not in consolidated_airports:
                        consolidated_airports[code] = {
                            'iata_code': code,
                            'names': [],
                            'cities': [],
                            'countries': [],
                            'country_codes': [],
                            'coordinates': [],
                            'sources': [],
                            'extra_data': {}
                        }
                    
                    # Collect all name variations
                    main_name = row.get(mapping['name_col'])
                    if pd.notna(main_name) and str(main_name).strip():
                        consolidated_airports[code]['names'].append(str(main_name).strip())
                    
                    alt_name = row.get(mapping.get('alt_name_col'))
                    if alt_name and pd.notna(alt_name) and str(alt_name).strip():
                        alt_name_str = str(alt_name).strip()
                        if alt_name_str not in consolidated_airports[code]['names']:
                            consolidated_airports[code]['names'].append(alt_name_str)
                    
                    # Collect geographic data
                    city = row.get(mapping['city_col'])
                    if pd.notna(city) and str(city).strip():
                        city_str = str(city).strip()
                        if city_str not in consolidated_airports[code]['cities']:
                            consolidated_airports[code]['cities'].append(city_str)
                    
                    country = row.get(mapping['country_col'])
                    if pd.notna(country) and str(country).strip():
                        country_str = str(country).strip()
                        if country_str not in consolidated_airports[code]['countries']:
                            consolidated_airports[code]['countries'].append(country_str)
                    
                    country_code = row.get(mapping.get('country_code_col'))
                    if country_code and pd.notna(country_code) and str(country_code).strip():
                        cc_str = str(country_code).strip()
                        if cc_str not in consolidated_airports[code]['country_codes']:
                            consolidated_airports[code]['country_codes'].append(cc_str)
                    
                    # Collect coordinates
                    lat_col = mapping.get('lat_col')
                    lon_col = mapping.get('lon_col')
                    
                    if lat_col and lon_col:  # Only process if both columns are defined
                        lat = row.get(lat_col)
                        lon = row.get(lon_col)
                        if pd.notna(lat) and pd.notna(lon):
                            try:
                                lat_float = float(lat)
                                lon_float = float(lon)
                                consolidated_airports[code]['coordinates'].append((lat_float, lon_float))
                            except (ValueError, TypeError):
                                pass
                    
                    # Track source
                    source = mapping['source']
                    if source not in consolidated_airports[code]['sources']:
                        consolidated_airports[code]['sources'].append(source)
                    
                    # Store additional data from ArcGIS sources
                    if source_key.startswith('arcgis'):
                        for col in df.columns:
                            if col not in mapping.values() and pd.notna(row.get(col)):
                                consolidated_airports[code]['extra_data'][col] = row[col]
        
        # Consolidate the collected data for each airport
        for code, airport_data in consolidated_airports.items():
            # Consolidate name (pick the best one)
            airport_data['consolidated_name'] = self.consolidate_airport_names(airport_data['names'])
            
            # Consolidate geographic data (prefer most common values)
            if airport_data['cities']:
                # Use the most common city name
                airport_data['consolidated_city'] = max(set(airport_data['cities']), 
                                                      key=airport_data['cities'].count)
            else:
                airport_data['consolidated_city'] = None
            
            if airport_data['countries']:
                # Use the most common country name
                airport_data['consolidated_country'] = max(set(airport_data['countries']), 
                                                         key=airport_data['countries'].count)
            else:
                airport_data['consolidated_country'] = None
            
            if airport_data['country_codes']:
                airport_data['consolidated_country_code'] = airport_data['country_codes'][0]
            else:
                airport_data['consolidated_country_code'] = None
            
            # Consolidate coordinates (use average if multiple)
            if airport_data['coordinates']:
                lats = [coord[0] for coord in airport_data['coordinates']]
                lons = [coord[1] for coord in airport_data['coordinates']]
                airport_data['consolidated_lat'] = sum(lats) / len(lats)
                airport_data['consolidated_lon'] = sum(lons) / len(lons)
                airport_data['has_coordinates'] = True
            else:
                airport_data['consolidated_lat'] = None
                airport_data['consolidated_lon'] = None
                airport_data['has_coordinates'] = False
            
            # Add quality metrics
            airport_data['source_count'] = len(airport_data['sources'])
            airport_data['is_well_documented'] = airport_data['source_count'] > 1
            airport_data['name_variations'] = len(set(airport_data['names']))
            
            # Calculate data quality score
            quality_score = 0.0
            if airport_data['has_coordinates']:
                quality_score += 0.4
            if airport_data['is_well_documented']:
                quality_score += 0.3
            if airport_data['consolidated_name'] and len(airport_data['consolidated_name']) > 5:
                quality_score += 0.2
            if airport_data['consolidated_city']:
                quality_score += 0.1
            
            airport_data['data_quality_score'] = quality_score
        
        logger.info(f"Consolidated {len(consolidated_airports)} unique airports from all sources")
        logger.info(f"Airports with coordinates: {sum(1 for a in consolidated_airports.values() if a['has_coordinates'])}")
        logger.info(f"Well-documented airports: {sum(1 for a in consolidated_airports.values() if a['is_well_documented'])}")
        
        return consolidated_airports
    
    def get_or_create_consolidated_airport(self, code: str, consolidated_data: Dict = None) -> URIRef:
        """Create airport using consolidated data to avoid duplicates"""
        if not code or pd.isna(code):
            return None
            
        code = str(code).upper().strip()
        
        if code in self.airports:
            return self.airports[code]
        
        # Create new airport with consolidated data
        airport_uri = self.klm[f"airport/{code}"]
        self.airports[code] = airport_uri
        
        # Add basic airport triples
        self.graph.add((airport_uri, RDF.type, self.klm.Airport))
        self.graph.add((airport_uri, self.klm.code, Literal(code)))
        
        # Add consolidated data if available
        if consolidated_data:
            # Add consolidated name
            if consolidated_data.get('consolidated_name'):
                name = consolidated_data['consolidated_name']
                self.graph.add((airport_uri, self.klm.name, Literal(name)))
                self.graph.add((airport_uri, self.schema.name, Literal(name)))
            
            # Add coordinates
            if consolidated_data.get('has_coordinates'):
                lat = consolidated_data['consolidated_lat']
                lon = consolidated_data['consolidated_lon']
                self.graph.add((airport_uri, self.geo.lat, Literal(lat, datatype=XSD.decimal)))
                self.graph.add((airport_uri, self.geo.long, Literal(lon, datatype=XSD.decimal)))
            
            # Add quality metrics
            self.graph.add((airport_uri, self.klm.sourceCount, 
                          Literal(consolidated_data['source_count'], datatype=XSD.integer)))
            self.graph.add((airport_uri, self.klm.hasCoordinates, 
                          Literal(consolidated_data['has_coordinates'], datatype=XSD.boolean)))
            self.graph.add((airport_uri, self.klm.isWellDocumented, 
                          Literal(consolidated_data['is_well_documented'], datatype=XSD.boolean)))
            self.graph.add((airport_uri, self.klm.dataQualityScore, 
                          Literal(consolidated_data['data_quality_score'], datatype=XSD.decimal)))
            
            # Add extra ArcGIS data
            extra_data = consolidated_data.get('extra_data', {})
            for key, value in extra_data.items():
                if pd.notna(value):
                    if key == 'elevation_ft':
                        self.graph.add((airport_uri, self.arcgis.elevation, 
                                      Literal(float(value), datatype=XSD.decimal)))
                    elif key == 'runway_length_m':
                        self.graph.add((airport_uri, self.arcgis.runwayLength, 
                                      Literal(float(value), datatype=XSD.decimal)))
                    elif key == 'runway_width_m':
                        self.graph.add((airport_uri, self.arcgis.runwayWidth, 
                                      Literal(float(value), datatype=XSD.decimal)))
                    elif key == 'runway_capacity':
                        self.graph.add((airport_uri, self.arcgis.runwayCapacity, Literal(str(value))))
                    elif key == 'hub_priority_score':
                        self.graph.add((airport_uri, self.arcgis.hubPriorityScore, 
                                      Literal(float(value), datatype=XSD.decimal)))
                    elif key == 'distance_from_ams_km':
                        self.graph.add((airport_uri, self.arcgis.distanceFromAMS, 
                                      Literal(float(value), datatype=XSD.decimal)))
                    elif key == 'strategic_distance':
                        self.graph.add((airport_uri, self.arcgis.strategicDistance, Literal(str(value))))
                    elif key == 'type':
                        if str(value).lower() == 'large_airport':
                            self.graph.add((airport_uri, RDF.type, self.arcgis.LargeAirport))
                        elif str(value).lower() == 'medium_airport':
                            self.graph.add((airport_uri, RDF.type, self.arcgis.MediumAirport))
            
            # Add geographic relationships
            if consolidated_data.get('consolidated_city') or consolidated_data.get('consolidated_country'):
                city_uri, country_uri = self.get_or_create_city_country(
                    consolidated_data.get('consolidated_city'),
                    consolidated_data.get('consolidated_country'),
                    consolidated_data.get('consolidated_country_code')
                )
                if city_uri:
                    self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
        
        # Initialize metrics tracking
        self.airport_metrics[code] = {
            'routes': set(),
            'flights': 0,
            'delayed_flights': 0,
            'total_delay_minutes': 0,
            'airlines': set()
        }
        
        # Special handling for known hubs
        if code == "AMS":
            self.graph.add((airport_uri, RDF.type, self.klm.HubAirport))
            self.graph.add((airport_uri, self.klm.isMainHub, Literal(True, datatype=XSD.boolean)))
        
        return airport_uri
    
    def create_enhanced_ontology(self):
        """Create enhanced ontology incorporating ArcGIS infrastructure concepts"""
        logger.info("Creating enhanced ontology with ArcGIS concepts")
        
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
        
        # Enhanced airport classes from ArcGIS data
        large_airport_class = self.arcgis.LargeAirport
        self.graph.add((large_airport_class, RDF.type, OWL.Class))
        self.graph.add((large_airport_class, RDFS.subClassOf, airport_class))
        self.graph.add((large_airport_class, RDFS.label, Literal("Large Airport")))
        
        medium_airport_class = self.arcgis.MediumAirport
        self.graph.add((medium_airport_class, RDF.type, OWL.Class))
        self.graph.add((medium_airport_class, RDFS.subClassOf, airport_class))
        self.graph.add((medium_airport_class, RDFS.label, Literal("Medium Airport")))
        
        # Other classes (airlines, routes, flights, etc.)
        for class_name, label in [
            ("Airline", "Airline"), ("Route", "Route"), ("Flight", "Flight"),
            ("City", "City"), ("Country", "Country"), ("Aircraft", "Aircraft")
        ]:
            class_uri = self.klm[class_name]
            self.graph.add((class_uri, RDF.type, OWL.Class))
            self.graph.add((class_uri, RDFS.label, Literal(label)))
        
        # Define enhanced properties
        self._define_enhanced_properties()
        
        logger.info("Enhanced ontology created")
    
    def _define_enhanced_properties(self):
        """Define enhanced data and object properties including ArcGIS concepts"""
        
        # Core identification properties
        properties = [
            # Data properties
            ("code", OWL.DatatypeProperty, XSD.string, "code"),
            ("name", OWL.DatatypeProperty, XSD.string, "name"),
            ("sourceCount", OWL.DatatypeProperty, XSD.integer, "number of data sources"),
            ("hasCoordinates", OWL.DatatypeProperty, XSD.boolean, "has geographic coordinates"),
            ("isWellDocumented", OWL.DatatypeProperty, XSD.boolean, "appears in multiple sources"),
            ("dataQualityScore", OWL.DatatypeProperty, XSD.decimal, "data quality score"),
            ("hubPotentialScore", OWL.DatatypeProperty, XSD.decimal, "hub potential score"),
            ("routeCount", OWL.DatatypeProperty, XSD.integer, "route count"),
            ("passengerVolume", OWL.DatatypeProperty, XSD.integer, "passenger volume"),
            ("delayRate", OWL.DatatypeProperty, XSD.decimal, "delay rate"),
            ("totalFlights", OWL.DatatypeProperty, XSD.integer, "total flights"),
            ("delayedFlights", OWL.DatatypeProperty, XSD.integer, "delayed flights"),
            ("connectivityIndex", OWL.DatatypeProperty, XSD.decimal, "connectivity index"),
            
            # Object properties
            ("locatedIn", OWL.ObjectProperty, None, "located in"),
            ("hasOrigin", OWL.ObjectProperty, None, "has origin"),
            ("hasDestination", OWL.ObjectProperty, None, "has destination"),
            ("hasHub", OWL.ObjectProperty, None, "has hub"),
            ("operates", OWL.ObjectProperty, None, "operates"),
            ("follows", OWL.ObjectProperty, None, "follows"),
            ("operatedWith", OWL.ObjectProperty, None, "operated with")
        ]
        
        for prop_name, prop_type, prop_range, label in properties:
            prop_uri = self.klm[prop_name]
            self.graph.add((prop_uri, RDF.type, prop_type))
            self.graph.add((prop_uri, RDFS.label, Literal(label)))
            if prop_range:
                self.graph.add((prop_uri, RDFS.range, prop_range))
        
        # Geographic properties
        self.graph.add((self.geo.lat, RDF.type, OWL.DatatypeProperty))
        self.graph.add((self.geo.long, RDF.type, OWL.DatatypeProperty))
        
        # ArcGIS infrastructure properties
        arcgis_properties = [
            ("elevation", XSD.decimal, "elevation"),
            ("runwayLength", XSD.decimal, "runway length"),
            ("runwayWidth", XSD.decimal, "runway width"),
            ("runwayCapacity", XSD.string, "runway capacity"),
            ("hubPriorityScore", XSD.decimal, "hub priority score"),
            ("distanceFromAMS", XSD.decimal, "distance from Amsterdam"),
            ("strategicDistance", XSD.string, "strategic distance category")
        ]
        
        for prop_name, prop_range, label in arcgis_properties:
            prop_uri = self.arcgis[prop_name]
            self.graph.add((prop_uri, RDF.type, OWL.DatatypeProperty))
            self.graph.add((prop_uri, RDFS.label, Literal(label)))
            self.graph.add((prop_uri, RDFS.range, prop_range))
    
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
                ams_uri = self.get_or_create_consolidated_airport("AMS")
                if ams_uri:
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
            
            # Link to airports using consolidated airport creation
            origin_uri = self.get_or_create_consolidated_airport(origin_code)
            dest_uri = self.get_or_create_consolidated_airport(dest_code)
            
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
        """Process and unify airport data from all sources with proper deduplication"""
        logger.info("Processing unified airport data with proper deduplication")
        
        # First, consolidate all airport data to eliminate duplicates
        consolidated_airports = self.consolidate_all_airport_data(data)
        
        # Store consolidated data for reference
        self.airport_data = consolidated_airports
        
        # Create airports using consolidated data
        airports_created = 0
        for code, airport_data in consolidated_airports.items():
            airport_uri = self.get_or_create_consolidated_airport(code, airport_data)
            if airport_uri:
                airports_created += 1
        
        logger.info(f"Created {airports_created} unique airports from consolidated data")
        logger.info("Airport deduplication completed successfully")
    
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
                if hub_score > 15 and airport_code != "AMS":
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
            delay_factor = max(0.5, 1.0 - delay_rate)
            base_score *= delay_factor
        
        # Enhanced scoring with ArcGIS infrastructure data
        infrastructure_bonus = 1.0
        
        # Get consolidated airport data for infrastructure metrics
        airport_data = self.airport_data.get(airport_code, {})
        extra_data = airport_data.get('extra_data', {})
        
        # Runway capacity bonus
        runway_capacity = extra_data.get('runway_capacity')
        if runway_capacity:
            if str(runway_capacity).lower() == 'large':
                infrastructure_bonus *= 1.3
            elif str(runway_capacity).lower() == 'medium':
                infrastructure_bonus *= 1.1
        
        # Strategic distance factor
        strategic_distance = extra_data.get('strategic_distance')
        if strategic_distance:
            distance_cat = str(strategic_distance).lower()
            if distance_cat == 'continental':
                infrastructure_bonus *= 1.2
            elif distance_cat == 'intercontinental':
                infrastructure_bonus *= 1.15
        
        # ArcGIS hub priority score integration
        hub_priority_score = extra_data.get('hub_priority_score')
        if hub_priority_score and pd.notna(hub_priority_score):
            try:
                priority_score = float(hub_priority_score)
                priority_factor = min(1.5, 1.0 + (priority_score / 10.0))
                infrastructure_bonus *= priority_factor
            except:
                pass
        
        base_score *= infrastructure_bonus
        
        # Data quality bonus
        data_quality = airport_data.get('data_quality_score', 0.5)
        base_score *= (0.8 + (data_quality * 0.4))  # Quality factor between 0.8 and 1.2
        
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
            'BHX': 13, 'EDI': 14, 'GLA': 9, 'NCL': 5, 'LBA': 4,
            # Major US airports for context
            'ATL': 110, 'LAX': 88, 'ORD': 84, 'DFW': 75, 'DEN': 69,
            'JFK': 62, 'SFO': 58, 'LAS': 51, 'SEA': 47, 'CLT': 50,
            'EWR': 46, 'MCO': 50, 'MIA': 45, 'PHX': 44, 'IAH': 45,
            'BOS': 42, 'MSP': 39, 'DTW': 34, 'PHL': 31, 'LGA': 31
        }
        
        airports_updated = 0
        for airport_code, volume_millions in passenger_data.items():
            airport_uri = self.airports.get(airport_code)
            if airport_uri:
                passenger_count = volume_millions * 1000000
                self.graph.add((airport_uri, self.klm.passengerVolume, 
                              Literal(passenger_count, datatype=XSD.integer)))
                airports_updated += 1
            else:
                # Create airport if it doesn't exist (for passenger data completeness)
                airport_uri = self.get_or_create_consolidated_airport(airport_code)
                if airport_uri:
                    passenger_count = volume_millions * 1000000
                    self.graph.add((airport_uri, self.klm.passengerVolume, 
                                  Literal(passenger_count, datatype=XSD.integer)))
                    airports_updated += 1
        
        logger.info(f"Added passenger data for {airports_updated} airports")
    
    def generate_enhanced_queries(self):
        """Generate enhanced SPARQL queries leveraging ArcGIS infrastructure data and proper deduplication"""
        logger.info("Generating enhanced SPARQL queries with proper deduplication")
        
        queries = {}
        
        # Enhanced hub selection query with infrastructure metrics and deduplication
        queries["enhanced_hub_selection_deduplicated"] = """
            # Enhanced second hub analysis with infrastructure data and proper deduplication
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX arcgis: <http://example.org/arcgis/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country 
                   ?routeCount ?passengerVolume ?hubScore ?dataQuality
                   ?runwayCapacity ?strategicDistance ?hubPriorityScore
                   ?distanceFromAMS ?delayRate ?sourceCount
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:routeCount ?routeCount ;
                        klm:hubPotentialScore ?hubScore ;
                        klm:dataQualityScore ?dataQuality ;
                        klm:sourceCount ?sourceCount .
                
                # Exclude current main hub
                FILTER NOT EXISTS { ?airport klm:isMainHub true }
                
                # Quality thresholds for hub consideration (ensures no duplicates)
                FILTER(?routeCount >= 5)
                FILTER(?dataQuality >= 0.6)
                FILTER(?sourceCount >= 1)
                
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
            ORDER BY DESC(?hubScore) DESC(?dataQuality) DESC(?sourceCount)
            LIMIT 15
        """
        
        # Data quality assessment to verify deduplication
        queries["deduplication_verification"] = """
            # Query to verify airport deduplication was successful
            PREFIX klm: <http://example.org/klm/>
            PREFIX arcgis: <http://example.org/arcgis/>
            
            SELECT ?airport ?name ?sourceCount ?dataQuality 
                   ?hasCoordinates ?isWellDocumented ?routeCount
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:sourceCount ?sourceCount ;
                        klm:dataQualityScore ?dataQuality ;
                        klm:hasCoordinates ?hasCoordinates ;
                        klm:isWellDocumented ?isWellDocumented .
                
                OPTIONAL { ?airport klm:routeCount ?routeCount }
                
                # Focus on airports that appear in multiple sources
                FILTER(?sourceCount > 1)
            }
            ORDER BY DESC(?sourceCount) DESC(?dataQuality)
        """
        
        # Route expansion with infrastructure constraints and deduplication
        queries["infrastructure_constrained_expansion_deduplicated"] = """
            # Route expansion considering infrastructure constraints with proper deduplication
            PREFIX klm: <http://example.org/klm/>
            PREFIX arcgis: <http://example.org/arcgis/>
            PREFIX sch: <http://example.org/schiphol/>
            
            SELECT ?airport ?name ?country ?passengerVolume ?runwayCapacity
                   ?strategicDistance ?dataQuality ?sourceCount
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:passengerVolume ?passengerVolume ;
                        klm:dataQualityScore ?dataQuality ;
                        klm:sourceCount ?sourceCount .
                
                # High passenger volume destinations with good data quality
                FILTER(?passengerVolume > 10000000)
                FILTER(?dataQuality >= 0.7)
                
                # Prefer well-documented airports (reduces chance of duplicates)
                FILTER(?sourceCount >= 2)
                
                # Infrastructure data
                OPTIONAL { ?airport arcgis:runwayCapacity ?runwayCapacity }
                OPTIONAL { ?airport arcgis:strategicDistance ?strategicDistance }
                
                # Not currently served by KLM from AMS
                FILTER NOT EXISTS {
                    ?klmRoute klm:hasOrigin <http://example.org/klm/airport/AMS> ;
                             klm:hasDestination ?airport .
                    ?klmAirline klm:operates ?klmRoute ;
                               klm:code "KL" .
                }
                
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?passengerVolume) DESC(?dataQuality) DESC(?sourceCount)
            LIMIT 20
        """
        
        # Delay analysis with deduplication verification
        queries["delay_analysis_deduplicated"] = """
            # Delay analysis with proper deduplication verification
            PREFIX klm: <http://example.org/klm/>
            PREFIX arcgis: <http://example.org/arcgis/>
            
            SELECT ?airport ?name ?country ?delayRate ?totalFlights ?delayedFlights
                   ?dataQuality ?sourceCount ?isWellDocumented
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:delayRate ?delayRate ;
                        klm:totalFlights ?totalFlights ;
                        klm:delayedFlights ?delayedFlights ;
                        klm:dataQualityScore ?dataQuality ;
                        klm:sourceCount ?sourceCount ;
                        klm:isWellDocumented ?isWellDocumented .
                
                # Quality filters to ensure reliable data
                FILTER(?totalFlights >= 10)
                FILTER(?dataQuality >= 0.6)
                FILTER(?isWellDocumented = true)
                
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?delayRate) DESC(?totalFlights)
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
        """Build the enhanced unified knowledge graph with proper airport deduplication"""
        logger.info("Building enhanced unified knowledge graph with proper airport deduplication")
        
        # Load all data from all sources
        data = self.load_all_data()
        
        # Create enhanced ontology
        self.create_enhanced_ontology()
        
        # Process all entities with proper deduplication (airports first)
        self.process_unified_airports(data)  # This now includes proper deduplication
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
        output_path = os.path.join(self.output_dir, f"deduplicated_unified_klm_hub_kg_{timestamp}.ttl")
        
        self.graph.serialize(destination=output_path, format="turtle")
        logger.info(f"Enhanced unified knowledge graph saved to {output_path}")
        
        # Also save as RDF/XML
        xml_output_path = os.path.join(self.output_dir, f"deduplicated_unified_klm_hub_kg_{timestamp}.rdf")
        self.graph.serialize(destination=xml_output_path, format="xml")
        logger.info(f"Enhanced unified knowledge graph also saved as RDF/XML to {xml_output_path}")
        
        # Generate detailed summary statistics
        stats = self._generate_detailed_stats()
        
        logger.info("Enhanced knowledge graph build complete:")
        logger.info(f"  - {stats['total_triples']} total triples")
        logger.info(f"  - {stats['airports']} airports (DEDUPLICATED)")
        logger.info(f"  - {stats['airlines']} airlines") 
        logger.info(f"  - {stats['routes']} routes")
        logger.info(f"  - {stats['cities']} cities")
        logger.info(f"  - {stats['countries']} countries")
        logger.info(f"  - {stats['aircraft_types']} aircraft types")
        logger.info(f"  - {stats['arcgis_enhanced_airports']} airports with ArcGIS data")
        logger.info(f"  - {stats['well_documented_airports']} well-documented airports")
        logger.info(f"  - {stats['airports_with_coordinates']} airports with coordinates")
        
        # Deduplication verification
        logger.info("Deduplication Summary:")
        logger.info(f"  - Average sources per airport: {stats['avg_sources_per_airport']:.1f}")
        logger.info(f"  - Airports from multiple sources: {stats['multi_source_airports']}")
        logger.info(f"  - Average data quality score: {stats['avg_data_quality']:.2f}")
        
        return stats
    
    def _generate_detailed_stats(self):
        """Generate detailed statistics about the knowledge graph"""
        stats = {
            'total_triples': len(self.graph),
            'airports': len(self.airports),
            'airlines': len(self.airlines),
            'routes': len(self.routes),
            'cities': len(self.cities),
            'countries': len(self.countries),
            'aircraft_types': len(self.aircraft),
            'output_file': None
        }
        
        # Calculate deduplication metrics
        total_sources = 0
        total_quality = 0.0
        airports_with_coordinates = 0
        well_documented_airports = 0
        arcgis_enhanced_airports = 0
        multi_source_airports = 0
        
        for airport_code, airport_data in self.airport_data.items():
            total_sources += airport_data['source_count']
            total_quality += airport_data['data_quality_score']
            
            if airport_data['has_coordinates']:
                airports_with_coordinates += 1
            
            if airport_data['is_well_documented']:
                well_documented_airports += 1
            
            if airport_data['source_count'] > 1:
                multi_source_airports += 1
            
            # Check for ArcGIS enhancement
            if any(source.startswith('arcgis') for source in airport_data['sources']):
                arcgis_enhanced_airports += 1
        
        stats['airports_with_coordinates'] = airports_with_coordinates
        stats['well_documented_airports'] = well_documented_airports
        stats['arcgis_enhanced_airports'] = arcgis_enhanced_airports
        stats['multi_source_airports'] = multi_source_airports
        
        if len(self.airport_data) > 0:
            stats['avg_sources_per_airport'] = total_sources / len(self.airport_data)
            stats['avg_data_quality'] = total_quality / len(self.airport_data)
        else:
            stats['avg_sources_per_airport'] = 0
            stats['avg_data_quality'] = 0
        
        return stats

def main():
    """Main function to build the enhanced unified knowledge graph with proper deduplication"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build enhanced unified KLM-Schiphol-ArcGIS knowledge graph with proper deduplication")
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
    
    # Create enhanced unified knowledge graph builder with deduplication
    builder = EnhancedUnifiedKnowledgeGraphBuilder(
        klm_processed_dir=args.klm_dir,
        schiphol_processed_dir=args.schiphol_dir,
        arcgis_processed_dir=args.arcgis_dir,
        output_dir=args.output_dir
    )
    
    # Build the enhanced unified knowledge graph
    try:
        stats = builder.build_enhanced_knowledge_graph()
        
        logger.info("🎉 Enhanced unified knowledge graph with proper deduplication successfully created!")
        logger.info("🔧 Key improvements in this version:")
        logger.info("   • PROPER AIRPORT DEDUPLICATION - eliminates duplicate entries like ATL")
        logger.info("   • Smart name consolidation from multiple sources")
        logger.info("   • Data quality scoring and source tracking")
        logger.info("   • Enhanced infrastructure data from ArcGIS Hub")
        logger.info("   • Coordinate averaging for better accuracy")
        logger.info("")
        logger.info("✅ Deduplication verification:")
        logger.info(f"   • {stats['multi_source_airports']} airports found in multiple sources")
        logger.info(f"   • Average {stats['avg_sources_per_airport']:.1f} sources per airport")
        logger.info(f"   • Average data quality score: {stats['avg_data_quality']:.2f}")
        logger.info("")
        logger.info("🔍 The deduplicated graph can now be used for:")
        logger.info("   • Accurate hub candidate identification (no duplicates)")
        logger.info("   • Reliable infrastructure-aware route planning")
        logger.info("   • Quality-weighted strategic analysis")
        logger.info("   • Comprehensive European airport coverage")
        logger.info("")
        logger.info("📁 Use the enhanced SPARQL queries to verify deduplication worked!")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error building enhanced unified knowledge graph: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit(main())