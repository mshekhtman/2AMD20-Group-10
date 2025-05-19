"""
KLM Knowledge Graph Builder

This script builds a knowledge graph from processed flight status data to support the
analysis of KLM's hub expansion research questions.
"""

import os
import pandas as pd
import logging
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, RDFS, XSD, OWL
import re
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kg_builder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KGBuilder")

class KLMKnowledgeGraphBuilder:
    """Knowledge Graph Builder for KLM flight data to support multi-hub analysis"""
    
    def __init__(self, processed_dir='data/KLM/processed', output_dir='data/knowledge_graph'):
        """Initialize the knowledge graph builder"""
        self.processed_dir = processed_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize RDF graph
        self.graph = Graph()
        
        # Define namespaces
        self.klm = Namespace("http://example.org/klm/")
        self.schema = Namespace("http://schema.org/")
        self.geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        self.dbo = Namespace("http://dbpedia.org/ontology/")
        
        # Bind namespaces
        self.graph.bind("klm", self.klm)
        self.graph.bind("schema", self.schema)
        self.graph.bind("geo", self.geo)
        self.graph.bind("dbo", self.dbo)
        self.graph.bind("owl", OWL)
        self.graph.bind("rdfs", RDFS)
        
        logger.info("KLM Knowledge Graph Builder initialized")
    
    def load_data(self):
        """Load processed data files"""
        logger.info("Loading processed data files")
        
        data = {}
        
        # Load airports
        airports_path = os.path.join(self.processed_dir, "airports.csv")
        if os.path.exists(airports_path):
            data["airports"] = pd.read_csv(airports_path)
            logger.info(f"Loaded {len(data['airports'])} airports")
        else:
            logger.warning("Airports file not found")
        
        # Load airlines
        airlines_path = os.path.join(self.processed_dir, "airlines.csv")
        if os.path.exists(airlines_path):
            data["airlines"] = pd.read_csv(airlines_path)
            logger.info(f"Loaded {len(data['airlines'])} airlines")
        else:
            logger.warning("Airlines file not found")
        
        # Load routes
        routes_path = os.path.join(self.processed_dir, "routes.csv")
        if os.path.exists(routes_path):
            data["routes"] = pd.read_csv(routes_path)
            logger.info(f"Loaded {len(data['routes'])} routes")
        else:
            logger.warning("Routes file not found")
        
        # Load flights
        flights_path = os.path.join(self.processed_dir, "flights.csv")
        if os.path.exists(flights_path):
            data["flights"] = pd.read_csv(flights_path)
            logger.info(f"Loaded {len(data['flights'])} flights")
        else:
            logger.warning("Flights file not found")
        
        return data
    
    def create_ontology(self):
        """Create the ontology for the knowledge graph focused on hub analysis"""
        logger.info("Creating ontology for multi-hub analysis")
        
        # Define classes
        
        # Airport class
        airport_class = self.klm.Airport
        self.graph.add((airport_class, RDF.type, OWL.Class))
        self.graph.add((airport_class, RDFS.label, Literal("Airport")))
        self.graph.add((airport_class, RDFS.comment, Literal("An airport facility")))
        
        # Hub Airport subclass - specifically for analyzing potential hub airports
        hub_airport_class = self.klm.HubAirport
        self.graph.add((hub_airport_class, RDF.type, OWL.Class))
        self.graph.add((hub_airport_class, RDFS.subClassOf, airport_class))
        self.graph.add((hub_airport_class, RDFS.label, Literal("Hub Airport")))
        self.graph.add((hub_airport_class, RDFS.comment, Literal("An airport that serves as a hub for an airline")))
        
        # Airline class
        airline_class = self.klm.Airline
        self.graph.add((airline_class, RDF.type, OWL.Class))
        self.graph.add((airline_class, RDFS.label, Literal("Airline")))
        self.graph.add((airline_class, RDFS.comment, Literal("An airline company")))
        
        # Route class
        route_class = self.klm.Route
        self.graph.add((route_class, RDF.type, OWL.Class))
        self.graph.add((route_class, RDFS.label, Literal("Route")))
        self.graph.add((route_class, RDFS.comment, Literal("A flight route between airports")))
        
        # Flight class
        flight_class = self.klm.Flight
        self.graph.add((flight_class, RDF.type, OWL.Class))
        self.graph.add((flight_class, RDFS.label, Literal("Flight")))
        self.graph.add((flight_class, RDFS.comment, Literal("A specific flight instance")))
        
        # City class
        city_class = self.klm.City
        self.graph.add((city_class, RDF.type, OWL.Class))
        self.graph.add((city_class, RDFS.label, Literal("City")))
        self.graph.add((city_class, RDFS.comment, Literal("A city")))
        
        # Country class
        country_class = self.klm.Country
        self.graph.add((country_class, RDF.type, OWL.Class))
        self.graph.add((country_class, RDFS.label, Literal("Country")))
        self.graph.add((country_class, RDFS.comment, Literal("A country")))
        
        # Aircraft class
        aircraft_class = self.klm.Aircraft
        self.graph.add((aircraft_class, RDF.type, OWL.Class))
        self.graph.add((aircraft_class, RDFS.label, Literal("Aircraft")))
        self.graph.add((aircraft_class, RDFS.comment, Literal("An aircraft type")))
        
        # Define data properties
        
        # Common properties
        code_prop = self.klm.code
        self.graph.add((code_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((code_prop, RDFS.label, Literal("code")))
        
        name_prop = self.klm.name
        self.graph.add((name_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((name_prop, RDFS.label, Literal("name")))
        
        # Geographic properties
        self.graph.add((self.geo.lat, RDF.type, OWL.DatatypeProperty))
        self.graph.add((self.geo.lat, RDFS.domain, airport_class))
        self.graph.add((self.geo.lat, RDFS.range, XSD.decimal))
        self.graph.add((self.geo.lat, RDFS.label, Literal("latitude")))
        
        self.graph.add((self.geo.long, RDF.type, OWL.DatatypeProperty))
        self.graph.add((self.geo.long, RDFS.domain, airport_class))
        self.graph.add((self.geo.long, RDFS.range, XSD.decimal))
        self.graph.add((self.geo.long, RDFS.label, Literal("longitude")))
        
        # Hub metrics properties - specific to analyzing potential hubs
        passenger_volume_prop = self.klm.passengerVolume
        self.graph.add((passenger_volume_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((passenger_volume_prop, RDFS.domain, airport_class))
        self.graph.add((passenger_volume_prop, RDFS.range, XSD.integer))
        self.graph.add((passenger_volume_prop, RDFS.label, Literal("passenger volume")))
        
        route_count_prop = self.klm.routeCount
        self.graph.add((route_count_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((route_count_prop, RDFS.domain, airport_class))
        self.graph.add((route_count_prop, RDFS.range, XSD.integer))
        self.graph.add((route_count_prop, RDFS.label, Literal("route count")))
        
        delay_rate_prop = self.klm.delayRate
        self.graph.add((delay_rate_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((delay_rate_prop, RDFS.domain, airport_class))
        self.graph.add((delay_rate_prop, RDFS.range, XSD.decimal))
        self.graph.add((delay_rate_prop, RDFS.label, Literal("delay rate")))
        
        # Flight properties
        flight_number_prop = self.klm.flightNumber
        self.graph.add((flight_number_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((flight_number_prop, RDFS.domain, flight_class))
        
        status_prop = self.klm.status
        self.graph.add((status_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((status_prop, RDFS.domain, flight_class))
        
        scheduled_departure_prop = self.klm.scheduledDeparture
        self.graph.add((scheduled_departure_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((scheduled_departure_prop, RDFS.domain, flight_class))
        self.graph.add((scheduled_departure_prop, RDFS.range, XSD.dateTime))
        
        scheduled_arrival_prop = self.klm.scheduledArrival
        self.graph.add((scheduled_arrival_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((scheduled_arrival_prop, RDFS.domain, flight_class))
        self.graph.add((scheduled_arrival_prop, RDFS.range, XSD.dateTime))
        
        estimated_arrival_prop = self.klm.estimatedArrival
        self.graph.add((estimated_arrival_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((estimated_arrival_prop, RDFS.domain, flight_class))
        self.graph.add((estimated_arrival_prop, RDFS.range, XSD.dateTime))
        
        # Route properties
        distance_prop = self.klm.distance
        self.graph.add((distance_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((distance_prop, RDFS.domain, route_class))
        self.graph.add((distance_prop, RDFS.range, XSD.decimal))
        
        scheduled_duration_prop = self.klm.scheduledDuration
        self.graph.add((scheduled_duration_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((scheduled_duration_prop, RDFS.domain, route_class))
        self.graph.add((scheduled_duration_prop, RDFS.range, XSD.duration))
        
        # Define object properties (relationships)
        located_in_prop = self.klm.locatedIn
        self.graph.add((located_in_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((located_in_prop, RDFS.label, Literal("located in")))
        
        origin_prop = self.klm.hasOrigin
        self.graph.add((origin_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((origin_prop, RDFS.label, Literal("has origin")))
        self.graph.add((origin_prop, RDFS.domain, route_class))
        self.graph.add((origin_prop, RDFS.range, airport_class))
        
        destination_prop = self.klm.hasDestination
        self.graph.add((destination_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((destination_prop, RDFS.label, Literal("has destination")))
        self.graph.add((destination_prop, RDFS.domain, route_class))
        self.graph.add((destination_prop, RDFS.range, airport_class))
        
        has_hub_prop = self.klm.hasHub
        self.graph.add((has_hub_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((has_hub_prop, RDFS.label, Literal("has hub")))
        self.graph.add((has_hub_prop, RDFS.domain, airline_class))
        self.graph.add((has_hub_prop, RDFS.range, hub_airport_class))
        
        operates_prop = self.klm.operates
        self.graph.add((operates_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((operates_prop, RDFS.label, Literal("operates")))
        self.graph.add((operates_prop, RDFS.domain, airline_class))
        
        follows_prop = self.klm.follows
        self.graph.add((follows_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((follows_prop, RDFS.label, Literal("follows")))
        self.graph.add((follows_prop, RDFS.domain, flight_class))
        self.graph.add((follows_prop, RDFS.range, route_class))
        
        operated_with_prop = self.klm.operatedWith
        self.graph.add((operated_with_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((operated_with_prop, RDFS.label, Literal("operated with")))
        self.graph.add((operated_with_prop, RDFS.domain, flight_class))
        self.graph.add((operated_with_prop, RDFS.range, aircraft_class))
        
        # Hub analysis specific properties
        potential_hub_score_prop = self.klm.potentialHubScore
        self.graph.add((potential_hub_score_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((potential_hub_score_prop, RDFS.domain, airport_class))
        self.graph.add((potential_hub_score_prop, RDFS.range, XSD.decimal))
        self.graph.add((potential_hub_score_prop, RDFS.label, Literal("potential hub score")))
        
        connectivity_index_prop = self.klm.connectivityIndex
        self.graph.add((connectivity_index_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((connectivity_index_prop, RDFS.domain, airport_class))
        self.graph.add((connectivity_index_prop, RDFS.range, XSD.decimal))
        self.graph.add((connectivity_index_prop, RDFS.label, Literal("connectivity index")))
        
        logger.info("Ontology created with hub analysis extensions")
    
    def add_airports(self, airports_df):
        """Add airports to the knowledge graph"""
        logger.info("Adding airports to the knowledge graph")
        
        airports_added = 0
        cities_added = set()
        countries_added = set()
        
        for _, row in airports_df.iterrows():
            # Skip if missing airport code
            if not pd.notna(row.get("airport_code")):
                continue
                
            # Create airport URI
            airport_code = row["airport_code"]
            airport_uri = self.klm[f"airport/{airport_code}"]
            
            # Add airport triples
            self.graph.add((airport_uri, RDF.type, self.klm.Airport))
            self.graph.add((airport_uri, self.klm.code, Literal(airport_code)))
            
            # Add airport name if available
            if pd.notna(row.get("airport_name")):
                self.graph.add((airport_uri, self.klm.name, Literal(row["airport_name"])))
                # Also add Schema.org name for better interoperability
                self.graph.add((airport_uri, self.schema.name, Literal(row["airport_name"])))
            
            # Add coordinates if available
            if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
                self.graph.add((airport_uri, self.geo.lat, Literal(row["latitude"], datatype=XSD.decimal)))
                self.graph.add((airport_uri, self.geo.long, Literal(row["longitude"], datatype=XSD.decimal)))
            
            # Add city relationship if available
            if pd.notna(row.get("city")):
                city_name = row["city"]
                # Create a slug for the city URI
                city_slug = re.sub(r'[^a-z0-9]', '_', city_name.lower())
                city_uri = self.klm[f"city/{city_slug}"]
                
                # Add city if not already added
                if city_slug not in cities_added:
                    self.graph.add((city_uri, RDF.type, self.klm.City))
                    self.graph.add((city_uri, self.klm.name, Literal(city_name)))
                    self.graph.add((city_uri, self.schema.name, Literal(city_name)))
                    cities_added.add(city_slug)
                
                # Link airport to city
                self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
                
                # Add country relationship if available
                if pd.notna(row.get("country")):
                    country_name = row["country"]
                    # Create a slug for the country URI
                    country_slug = re.sub(r'[^a-z0-9]', '_', country_name.lower())
                    country_uri = self.klm[f"country/{country_slug}"]
                    
                    # Add country if not already added
                    if country_slug not in countries_added:
                        self.graph.add((country_uri, RDF.type, self.klm.Country))
                        self.graph.add((country_uri, self.klm.name, Literal(country_name)))
                        self.graph.add((country_uri, self.schema.name, Literal(country_name)))
                        countries_added.add(country_slug)
                    
                    # Link city to country
                    self.graph.add((city_uri, self.klm.locatedIn, country_uri))
            
            # Add special properties for Schiphol (KLM's main hub)
            if airport_code == "AMS":
                self.graph.add((airport_uri, RDF.type, self.klm.HubAirport))
                klm_uri = self.klm[f"airline/KL"]
                self.graph.add((klm_uri, self.klm.hasHub, airport_uri))
                self.graph.add((airport_uri, self.klm.isMainHub, Literal(True, datatype=XSD.boolean)))
            
            airports_added += 1
        
        logger.info(f"Added {airports_added} airports, {len(cities_added)} cities, and {len(countries_added)} countries to the knowledge graph")
    
    def add_airlines(self, airlines_df):
        """Add airlines to the knowledge graph"""
        logger.info("Adding airlines to the knowledge graph")
        
        airlines_added = 0
        
        for _, row in airlines_df.iterrows():
            # Skip if missing airline code
            if not pd.notna(row.get("airline_code")):
                continue
                
            # Create airline URI
            airline_code = row["airline_code"]
            airline_uri = self.klm[f"airline/{airline_code}"]
            
            # Add airline triples
            self.graph.add((airline_uri, RDF.type, self.klm.Airline))
            self.graph.add((airline_uri, self.klm.code, Literal(airline_code)))
            
            # Add airline name if available
            if pd.notna(row.get("airline_name")):
                self.graph.add((airline_uri, self.klm.name, Literal(row["airline_name"])))
                self.graph.add((airline_uri, self.schema.name, Literal(row["airline_name"])))
            
            # Add special properties for KLM
            if airline_code == "KL":
                self.graph.add((airline_uri, self.klm.isCurrentProject, Literal(True, datatype=XSD.boolean)))
                # Link to Schiphol as main hub
                schiphol_uri = self.klm[f"airport/AMS"]
                self.graph.add((airline_uri, self.klm.hasHub, schiphol_uri))
            
            airlines_added += 1
        
        logger.info(f"Added {airlines_added} airlines to the knowledge graph")
    
    def add_routes(self, routes_df):
        """Add routes to the knowledge graph"""
        logger.info("Adding routes to the knowledge graph")
        
        routes_added = 0
        
        for _, row in routes_df.iterrows():
            # Skip if missing origin or destination
            if not pd.notna(row.get("origin")) or not pd.notna(row.get("destination")):
                continue
                
            # Get origin and destination
            origin = row["origin"]
            destination = row["destination"]
            
            # Create route URI
            route_uri = self.klm[f"route/{origin}-{destination}"]
            
            # Add route triples
            self.graph.add((route_uri, RDF.type, self.klm.Route))
            
            # Add origin and destination relationships
            origin_uri = self.klm[f"airport/{origin}"]
            destination_uri = self.klm[f"airport/{destination}"]
            
            self.graph.add((route_uri, self.klm.hasOrigin, origin_uri))
            self.graph.add((route_uri, self.klm.hasDestination, destination_uri))
            
            # Add airline relationship if available
            if pd.notna(row.get("airline_code")):
                airline_uri = self.klm[f"airline/{row['airline_code']}"]
                self.graph.add((airline_uri, self.klm.operates, route_uri))
            
            # Add duration if available
            if pd.notna(row.get("scheduled_duration")):
                duration = row["scheduled_duration"]
                # Convert PT1H30M format to ISO duration if needed
                if isinstance(duration, str) and duration.startswith("PT"):
                    self.graph.add((route_uri, self.klm.scheduledDuration, Literal(duration, datatype=XSD.duration)))
                elif isinstance(duration, str):
                    # Try to parse other duration formats
                    try:
                        # If it's in HH:MM format
                        if ":" in duration:
                            hours, minutes = duration.split(":")
                            iso_duration = f"PT{hours}H{minutes}M"
                            self.graph.add((route_uri, self.klm.scheduledDuration, Literal(iso_duration, datatype=XSD.duration)))
                        # If it's in hours only
                        elif "h" in duration.lower():
                            hours = duration.lower().replace("h", "").strip()
                            iso_duration = f"PT{hours}H"
                            self.graph.add((route_uri, self.klm.scheduledDuration, Literal(iso_duration, datatype=XSD.duration)))
                    except:
                        # If parsing fails, just store as string
                        self.graph.add((route_uri, self.klm.rawDuration, Literal(duration)))
            
            routes_added += 1
        
        logger.info(f"Added {routes_added} routes to the knowledge graph")
    
    def add_flights(self, flights_df):
        """Add flights to the knowledge graph"""
        logger.info("Adding flights to the knowledge graph")
        
        flights_added = 0
        delay_data = {}  # For tracking delays by destination
        
        for _, row in flights_df.iterrows():
            # Skip rows without flight number or ID
            if not pd.notna(row.get("flight_number")) or not pd.notna(row.get("flight_id")):
                continue
            
            # Create flight URI
            flight_id = row["flight_id"]
            flight_uri = self.klm[f"flight/{flight_id}"]
            
            # Add flight triples
            self.graph.add((flight_uri, RDF.type, self.klm.Flight))
            
            # Add flight number
            self.graph.add((flight_uri, self.klm.flightNumber, Literal(row["flight_number"], datatype=XSD.string)))
            
            # Add flight date if available
            if pd.notna(row.get("flight_date")):
                self.graph.add((flight_uri, self.klm.flightDate, Literal(row["flight_date"], datatype=XSD.date)))
            
            # Add status if available
            if pd.notna(row.get("status")):
                self.graph.add((flight_uri, self.klm.status, Literal(row["status"])))
            
            # Add leg status if available
            if pd.notna(row.get("leg_status")):
                self.graph.add((flight_uri, self.klm.legStatus, Literal(row["leg_status"])))
            
            # Add airline relationship if available
            if pd.notna(row.get("airline_code")):
                airline_uri = self.klm[f"airline/{row['airline_code']}"]
                self.graph.add((airline_uri, self.klm.operates, flight_uri))
            
            # Add route relationship if available
            if pd.notna(row.get("departure_airport_code")) and pd.notna(row.get("arrival_airport_code")):
                origin = row["departure_airport_code"]
                destination = row["arrival_airport_code"]
                route_uri = self.klm[f"route/{origin}-{destination}"]
                
                self.graph.add((flight_uri, self.klm.follows, route_uri))
                
                # Track destinations for delay analysis (used in client research question 3)
                if destination not in delay_data:
                    delay_data[destination] = {"total": 0, "delayed": 0}
                
                delay_data[destination]["total"] += 1
                
                # Check if there's a delay
                is_delayed = False
                if pd.notna(row.get("scheduled_arrival_time")) and pd.notna(row.get("estimated_arrival_time")):
                    # Simple delay check (in a real system, you'd need proper datetime parsing)
                    if row["estimated_arrival_time"] > row["scheduled_arrival_time"]:
                        is_delayed = True
                        delay_data[destination]["delayed"] += 1
                
                # Add delay info to flight
                self.graph.add((flight_uri, self.klm.isDelayed, Literal(is_delayed, datatype=XSD.boolean)))
            
            # Add departure and arrival times if available
            if pd.notna(row.get("scheduled_departure_time")):
                self.graph.add((flight_uri, self.klm.scheduledDeparture, Literal(row["scheduled_departure_time"], datatype=XSD.dateTime)))
            
            if pd.notna(row.get("scheduled_arrival_time")):
                self.graph.add((flight_uri, self.klm.scheduledArrival, Literal(row["scheduled_arrival_time"], datatype=XSD.dateTime)))
            
            if pd.notna(row.get("estimated_arrival_time")):
                self.graph.add((flight_uri, self.klm.estimatedArrival, Literal(row["estimated_arrival_time"], datatype=XSD.dateTime)))
            
            # Add aircraft information if available
            if pd.notna(row.get("aircraft_type_code")):
                aircraft_code = row["aircraft_type_code"]
                aircraft_uri = self.klm[f"aircraft/{aircraft_code}"]
                
                # Add aircraft data
                self.graph.add((aircraft_uri, RDF.type, self.klm.Aircraft))
                self.graph.add((aircraft_uri, self.klm.code, Literal(aircraft_code)))
                
                if pd.notna(row.get("aircraft_type_name")):
                    self.graph.add((aircraft_uri, self.klm.name, Literal(row["aircraft_type_name"])))
                
                # Link flight to aircraft
                self.graph.add((flight_uri, self.klm.operatedWith, aircraft_uri))
            
            flights_added += 1
        
        # Add delay statistics to airports (for research question 3)
        for airport_code, stats in delay_data.items():
            if stats["total"] > 0:
                airport_uri = self.klm[f"airport/{airport_code}"]
                delay_rate = stats["delayed"] / stats["total"]
                self.graph.add((airport_uri, self.klm.delayRate, Literal(delay_rate, datatype=XSD.decimal)))
                self.graph.add((airport_uri, self.klm.totalFlights, Literal(stats["total"], datatype=XSD.integer)))
                self.graph.add((airport_uri, self.klm.delayedFlights, Literal(stats["delayed"], datatype=XSD.integer)))
        
        logger.info(f"Added {flights_added} flights to the knowledge graph with delay statistics")
    
    def calculate_hub_metrics(self):
        """Calculate and add hub potential metrics to airports"""
        logger.info("Calculating hub potential metrics")
        
        # Count routes per airport (connectivity metric)
        airport_routes = {}
        routes_query = """
            SELECT ?airport ?route
            WHERE {
                {
                    ?route klm:hasOrigin ?airport .
                }
                UNION
                {
                    ?route klm:hasDestination ?airport .
                }
            }
        """
        
        results = self.graph.query(routes_query)
        for row in results:
            airport = str(row.airport)
            if airport not in airport_routes:
                airport_routes[airport] = set()
            airport_routes[airport].add(str(row.route))
        
             # Calculate hub metrics for each airport
        for airport_uri, routes in airport_routes.items():
               airport = URIRef(airport_uri)
               
               # Add route count
               route_count = len(routes)
               self.graph.add((airport, self.klm.routeCount, Literal(route_count, datatype=XSD.integer)))
               
               # Calculate a hub potential score
               # This is a simple metric combining connectivity with some other factors
               hub_score = route_count
               
               # Check if we have delay data - fewer delays is better for a hub
               delay_rate_query = f"""
                   SELECT ?delayRate
                   WHERE {{
                       <{airport_uri}> klm:delayRate ?delayRate .
                   }}
               """
               delay_results = list(self.graph.query(delay_rate_query))
               
               if delay_results:
                   # Lower delay rate increases hub score
                   delay_rate = float(delay_results[0].delayRate)
                   hub_score *= (1.0 - delay_rate)
               
               # Add hub score to the airport
               self.graph.add((airport, self.klm.hubPotentialScore, Literal(hub_score, datatype=XSD.decimal)))
               
               # If we have a high hub score, tag it as a potential hub
               if hub_score > 5:  # Arbitrary threshold for illustration
                   self.graph.add((airport, RDF.type, self.klm.PotentialHubAirport))
       
        logger.info("Hub metrics calculated and added to the knowledge graph")
   
    def add_eurostat_data(self):
       """
       Add Eurostat data to enrich the knowledge graph
       This is a placeholder - in a real implementation, you would load and integrate
       Eurostat data about passenger volumes, etc.
       """
       logger.info("Placeholder for adding Eurostat data to the knowledge graph")
       
       # Simulate adding passenger volume data to major airports
       major_airports = {
           "AMS": 71000000,  # Amsterdam Schiphol
           "CDG": 76000000,  # Paris Charles de Gaulle
           "FRA": 70000000,  # Frankfurt
           "LHR": 80000000,  # London Heathrow
           "MAD": 61000000,  # Madrid
           "FCO": 48000000,  # Rome
           "IST": 68000000,  # Istanbul
           "BRU": 26000000,  # Brussels
           "VIE": 31000000,  # Vienna
           "CPH": 30000000,  # Copenhagen
           # Add more as needed
       }
       
       for code, volume in major_airports.items():
           airport_uri = self.klm[f"airport/{code}"]
           self.graph.add((airport_uri, self.klm.passengerVolume, Literal(volume, datatype=XSD.integer)))
           
           # Update hub potential score with passenger volume
           hub_score_query = f"""
               SELECT ?hubScore
               WHERE {{
                   <{str(airport_uri)}> klm:hubPotentialScore ?hubScore .
               }}
           """
           results = list(self.graph.query(hub_score_query))
           
           if results:
               current_score = float(results[0].hubScore)
               # Adjust score based on passenger volume (normalized)
               normalized_volume = volume / 10000000  # Scale down
               new_score = current_score * (1 + normalized_volume / 10)
               
               # Update hub score
               self.graph.remove((airport_uri, self.klm.hubPotentialScore, None))
               self.graph.add((airport_uri, self.klm.hubPotentialScore, Literal(new_score, datatype=XSD.decimal)))
       
       logger.info(f"Added passenger volume data for {len(major_airports)} major airports")
   
    def build_knowledge_graph(self):
       """Build the knowledge graph from processed data"""
       logger.info("Building knowledge graph for KLM hub expansion analysis")
       
       # Create ontology
       self.create_ontology()
       
       # Load data
       data = self.load_data()
       
       # Add airports
       if "airports" in data:
           self.add_airports(data["airports"])
       
       # Add airlines
       if "airlines" in data:
           self.add_airlines(data["airlines"])
       
       # Add routes
       if "routes" in data:
           self.add_routes(data["routes"])
       
       # Add flights
       if "flights" in data:
           self.add_flights(data["flights"])
       
       # Calculate hub metrics
       self.calculate_hub_metrics()
       
       # Add Eurostat data (placeholder)
       self.add_eurostat_data()
       
       # Save knowledge graph
       timestamp = datetime.now().strftime("%Y%m%d")
       output_path = os.path.join(self.output_dir, f"klm_hub_expansion_kg_{timestamp}.ttl")
       self.graph.serialize(destination=output_path, format="turtle")
       
       logger.info(f"Knowledge graph saved to {output_path}")
       
       # Also save as RDF/XML for compatibility with some tools
       xml_output_path = os.path.join(self.output_dir, f"klm_hub_expansion_kg_{timestamp}.rdf")
       self.graph.serialize(destination=xml_output_path, format="xml")
       
       logger.info(f"Knowledge graph also saved as RDF/XML to {xml_output_path}")
       
       # Generate sample queries for client research questions
       self.generate_research_queries()
       
       # Return stats
       return {
           "triples": len(self.graph),
           "file_path": output_path
       }
   
    def generate_research_queries(self):
       """Generate sample SPARQL queries to address client research questions"""
       logger.info("Generating sample SPARQL queries for client research questions")
       
       queries = {}
       
       # Research Question 1: Which other airport would be a suitable fit for a second hub?
       queries["second_hub"] = """
           # Query to find suitable airports for a second KLM hub
           PREFIX klm: <http://example.org/klm/>
           PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
           
           SELECT ?airport ?name ?country ?hubScore ?routeCount ?passengerVolume ?delayRate
           WHERE {
               ?airport a klm:Airport ;
                       klm:code ?code ;
                       klm:name ?name ;
                       klm:hubPotentialScore ?hubScore ;
                       klm:routeCount ?routeCount .
                       
               # Filter out Schiphol (current hub)
               FILTER(?code != "AMS")
               
               # Only consider airports with significant connectivity
               FILTER(?routeCount > 3)
               
               OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
               OPTIONAL { ?airport klm:delayRate ?delayRate }
               
               # Get country information if available
               OPTIONAL { 
                   ?airport klm:locatedIn ?city .
                   ?city klm:locatedIn ?countryUri .
                   ?countryUri klm:name ?country .
               }
           }
           ORDER BY DESC(?hubScore)
           LIMIT 10
       """
       
       # Research Question 2: What airports are we not flying to but could expand to?
       queries["potential_destinations"] = """
           # Query to find potential new destination airports based on passenger metrics
           PREFIX klm: <http://example.org/klm/>
           PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           
           # First, find all airports KLM currently flies to
           SELECT ?airport ?name ?country ?passengerVolume
           WHERE {
               # Airports with significant passenger volume
               ?airport a klm:Airport ;
                       klm:name ?name ;
                       klm:passengerVolume ?passengerVolume .
               
               OPTIONAL { 
                   ?airport klm:locatedIn ?city .
                   ?city klm:locatedIn ?countryUri .
                   ?countryUri klm:name ?country .
               }
               
               # High passenger volume
               FILTER(?passengerVolume > 10000000)
               
               # Not already served by KLM
               FILTER NOT EXISTS {
                   ?route klm:hasDestination ?airport .
                   ?klmAirline klm:operates ?route .
                   ?klmAirline klm:code "KL" .
               }
           }
           ORDER BY DESC(?passengerVolume)
           LIMIT 15
       """
       
       # Research Question 3: Correlations between destinations and delays
       queries["delay_analysis"] = """
           # Query to analyze delay patterns by destination
           PREFIX klm: <http://example.org/klm/>
           PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
           
           SELECT ?airport ?name ?country ?delayRate ?totalFlights ?delayedFlights
           WHERE {
               ?airport a klm:Airport ;
                       klm:code ?code ;
                       klm:name ?name ;
                       klm:delayRate ?delayRate ;
                       klm:totalFlights ?totalFlights ;
                       klm:delayedFlights ?delayedFlights .
               
               # Only include destinations with significant flight volume
               FILTER(?totalFlights >= 5)
               
               OPTIONAL { 
                   ?airport klm:locatedIn ?city .
                   ?city klm:locatedIn ?countryUri .
                   ?countryUri klm:name ?country .
               }
           }
           ORDER BY DESC(?delayRate)
       """
       
       # Save queries to files
       queries_dir = os.path.join(self.output_dir, "queries")
       os.makedirs(queries_dir, exist_ok=True)
       
       for name, query in queries.items():
           query_path = os.path.join(queries_dir, f"{name}_query.sparql")
           with open(query_path, 'w', encoding='utf-8') as f:
               f.write(query)
           logger.info(f"Saved research query '{name}' to {query_path}")
       
       return queries

def main():
   """Main function"""
   import argparse
   
   parser = argparse.ArgumentParser(description="Build a knowledge graph from KLM flight data")
   parser.add_argument("--processed-dir", default="data/KLM/processed", 
                     help="Directory containing processed data files")
   parser.add_argument("--output-dir", default="data/knowledge_graph",
                     help="Directory to save the knowledge graph files")
   
   args = parser.parse_args()
   
   # Create knowledge graph builder
   builder = KLMKnowledgeGraphBuilder(processed_dir=args.processed_dir, output_dir=args.output_dir)
   
   # Build knowledge graph
   stats = builder.build_knowledge_graph()
   
   logger.info(f"Knowledge graph built with {stats['triples']} triples")
   logger.info(f"Knowledge graph files saved to {args.output_dir}")
   logger.info("The knowledge graph can now be used to analyze KLM's hub expansion opportunities")

if __name__ == "__main__":
   main()