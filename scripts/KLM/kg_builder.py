"""
KLM Knowledge Graph Builder

This script builds a knowledge graph from processed flight status data.
"""

import os
import pandas as pd
import logging
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, RDFS, XSD, OWL
import re

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

class KGBuilder:
    """Knowledge Graph Builder for KLM flight data"""
    
    def __init__(self, processed_dir='data/processed', output_dir='data/knowledge_graph'):
        """Initialize the knowledge graph builder"""
        self.processed_dir = processed_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize RDF graph
        self.graph = Graph()
        
        # Define namespaces
        self.klm = Namespace("http://example.org/klm/")
        self.schema = Namespace("http://schema.org/")
        
        # Bind namespaces
        self.graph.bind("klm", self.klm)
        self.graph.bind("schema", self.schema)
        self.graph.bind("owl", OWL)
        self.graph.bind("rdfs", RDFS)
        
        logger.info("Knowledge Graph Builder initialized")
    
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
        """Create the ontology for the knowledge graph"""
        logger.info("Creating ontology")
        
        # Define classes
        
        # Airport class
        airport_class = self.klm.Airport
        self.graph.add((airport_class, RDF.type, OWL.Class))
        self.graph.add((airport_class, RDFS.label, Literal("Airport")))
        self.graph.add((airport_class, RDFS.comment, Literal("An airport facility")))
        
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
        
        # Define properties
        
        # Common properties
        code_prop = self.klm.code
        self.graph.add((code_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((code_prop, RDFS.label, Literal("code")))
        
        name_prop = self.klm.name
        self.graph.add((name_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((name_prop, RDFS.label, Literal("name")))
        
        # Airport properties
        latitude_prop = self.klm.latitude
        self.graph.add((latitude_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((latitude_prop, RDFS.domain, airport_class))
        self.graph.add((latitude_prop, RDFS.range, XSD.decimal))
        
        longitude_prop = self.klm.longitude
        self.graph.add((longitude_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((longitude_prop, RDFS.domain, airport_class))
        self.graph.add((longitude_prop, RDFS.range, XSD.decimal))
        
        # Flight properties
        flight_number_prop = self.klm.flightNumber
        self.graph.add((flight_number_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((flight_number_prop, RDFS.domain, flight_class))
        
        status_prop = self.klm.status
        self.graph.add((status_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((status_prop, RDFS.domain, flight_class))
        
        # Object properties (relationships)
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
        
        operates_prop = self.klm.operates
        self.graph.add((operates_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((operates_prop, RDFS.label, Literal("operates")))
        self.graph.add((operates_prop, RDFS.domain, airline_class))
        self.graph.add((operates_prop, RDFS.range, flight_class))
        
        follows_prop = self.klm.follows
        self.graph.add((follows_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((follows_prop, RDFS.label, Literal("follows")))
        self.graph.add((follows_prop, RDFS.domain, flight_class))
        self.graph.add((follows_prop, RDFS.range, route_class))
        
        logger.info("Ontology created")

def add_airports(self, airports_df):
    """Add airports to the knowledge graph"""
    logger.info("Adding airports to the knowledge graph")
    
    airports_added = 0
    cities_added = set()
    countries_added = set()
    
    for _, row in airports_df.iterrows():
        # Create airport URI
        airport_code = row["airport_code"]
        airport_uri = self.klm[f"airport/{airport_code}"]
        
        # Add airport triples
        self.graph.add((airport_uri, RDF.type, self.klm.Airport))
        self.graph.add((airport_uri, self.klm.code, Literal(airport_code)))
        
        # Add airport name if available
        if pd.notna(row.get("airport_name")):
            self.graph.add((airport_uri, self.klm.name, Literal(row["airport_name"])))
        
        # Add coordinates if available
        if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
            self.graph.add((airport_uri, self.klm.latitude, Literal(row["latitude"], datatype=XSD.decimal)))
            self.graph.add((airport_uri, self.klm.longitude, Literal(row["longitude"], datatype=XSD.decimal)))
        
        # Add city relationship if available
        if pd.notna(row.get("city_name")) and pd.notna(row.get("city_code")):
            city_code = row["city_code"]
            city_uri = self.klm[f"city/{city_code}"]
            
            # Add city if not already added
            if city_code not in cities_added:
                self.graph.add((city_uri, RDF.type, self.klm.City))
                self.graph.add((city_uri, self.klm.code, Literal(city_code)))
                self.graph.add((city_uri, self.klm.name, Literal(row["city_name"])))
                cities_added.add(city_code)
            
            # Link airport to city
            self.graph.add((airport_uri, self.klm.locatedIn, city_uri))
            
            # Add country relationship if available
            if pd.notna(row.get("country_name")) and pd.notna(row.get("country_code")):
                country_code = row["country_code"]
                country_uri = self.klm[f"country/{country_code}"]
                
                # Add country if not already added
                if country_code not in countries_added:
                    self.graph.add((country_uri, RDF.type, self.klm.Country))
                    self.graph.add((country_uri, self.klm.code, Literal(country_code)))
                    self.graph.add((country_uri, self.klm.name, Literal(row["country_name"])))
                    countries_added.add(country_code)
                
                # Link city to country
                self.graph.add((city_uri, self.klm.locatedIn, country_uri))
        
        airports_added += 1
    
    logger.info(f"Added {airports_added} airports, {len(cities_added)} cities, and {len(countries_added)} countries to the knowledge graph")

def add_airlines(self, airlines_df):
    """Add airlines to the knowledge graph"""
    logger.info("Adding airlines to the knowledge graph")
    
    airlines_added = 0
    
    for _, row in airlines_df.iterrows():
        # Create airline URI
        airline_code = row["airline_code"]
        airline_uri = self.klm[f"airline/{airline_code}"]
        
        # Add airline triples
        self.graph.add((airline_uri, RDF.type, self.klm.Airline))
        self.graph.add((airline_uri, self.klm.code, Literal(airline_code)))
        
        # Add airline name if available
        if pd.notna(row.get("airline_name")):
            self.graph.add((airline_uri, self.klm.name, Literal(row["airline_name"])))
        
        airlines_added += 1
    
    logger.info(f"Added {airlines_added} airlines to the knowledge graph")

def add_routes(self, routes_df):
    """Add routes to the knowledge graph"""
    logger.info("Adding routes to the knowledge graph")
    
    routes_added = 0
    
    for _, row in routes_df.iterrows():
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
            # Convert PT1H30M format to ISO duration
            if isinstance(duration, str) and duration.startswith("PT"):
                self.graph.add((route_uri, self.klm.scheduledDuration, Literal(duration, datatype=XSD.duration)))
        
        routes_added += 1
    
    logger.info(f"Added {routes_added} routes to the knowledge graph")

def add_flights(self, flights_df):
    """Add flights to the knowledge graph"""
    logger.info("Adding flights to the knowledge graph")
    
    flights_added = 0
    
    for _, row in flights_df.iterrows():
        # Skip rows without flight number
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
        
        # Add departure and arrival times if available
        if pd.notna(row.get("scheduled_departure_time")):
            self.graph.add((flight_uri, self.klm.scheduledDeparture, Literal(row["scheduled_departure_time"], datatype=XSD.dateTime)))
        
        if pd.notna(row.get("scheduled_arrival_time")):
            self.graph.add((flight_uri, self.klm.scheduledArrival, Literal(row["scheduled_arrival_time"], datatype=XSD.dateTime)))
        
        if pd.notna(row.get("estimated_arrival_time")):
            self.graph.add((flight_uri, self.klm.estimatedArrival, Literal(row["estimated_arrival_time"], datatype=XSD.dateTime)))
        
        # Add aircraft information if available
        if pd.notna(row.get("aircraft_type_code")) and pd.notna(row.get("aircraft_type_name")):
            aircraft_uri = self.klm[f"aircraft/{row['aircraft_type_code']}"]
            self.graph.add((aircraft_uri, RDF.type, self.klm.Aircraft))
            self.graph.add((aircraft_uri, self.klm.code, Literal(row["aircraft_type_code"])))
            self.graph.add((aircraft_uri, self.klm.name, Literal(row["aircraft_type_name"])))
            
            # Link flight to aircraft
            self.graph.add((flight_uri, self.klm.operatedWith, aircraft_uri))
        
        flights_added += 1
    
    logger.info(f"Added {flights_added} flights to the knowledge graph")

def build_knowledge_graph(self):
    """Build the knowledge graph from processed data"""
    logger.info("Building knowledge graph")
    
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
    
    # Save knowledge graph
    output_path = os.path.join(self.output_dir, "klm_knowledge_graph.ttl")
    self.graph.serialize(destination=output_path, format="turtle")
    
    logger.info(f"Knowledge graph saved to {output_path}")
    
    # Also save as RDF/XML for compatibility with some tools
    xml_output_path = os.path.join(self.output_dir, "klm_knowledge_graph.rdf")
    self.graph.serialize(destination=xml_output_path, format="xml")
    
    logger.info(f"Knowledge graph also saved as RDF/XML to {xml_output_path}")
    
    # Return stats
    return {
        "triples": len(self.graph),
        "file_path": output_path
    }

def main():
    """Main function"""
    # Create knowledge graph builder
    builder = KGBuilder()
    
    # Build knowledge graph
    stats = builder.build_knowledge_graph()
    
    logger.info(f"Knowledge graph built with {stats['triples']} triples")

if __name__ == "__main__":
    main()