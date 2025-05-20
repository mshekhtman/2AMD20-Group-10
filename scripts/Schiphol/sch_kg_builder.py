"""
Schiphol Knowledge Graph Builder

This script builds a knowledge graph from processed Schiphol API data to complement
the KLM flight data for analyzing hub expansion opportunities.
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
        logging.FileHandler("schiphol_kg_builder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SchipholKGBuilder")

class SchipholKnowledgeGraphBuilder:
    """Knowledge Graph Builder for Schiphol Airport data to support multi-hub analysis"""
    
    def __init__(self, processed_dir='data/Schiphol/processed', output_dir='data/Schiphol/knowledge_graph'):
        """Initialize the knowledge graph builder"""
        self.processed_dir = processed_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize RDF graph - we'll merge with KLM graph later
        self.graph = Graph()
        
        # Define namespaces - using the same as KLM for consistency
        self.sch = Namespace("http://example.org/schiphol/")
        self.klm = Namespace("http://example.org/klm/")  # For compatibility with KLM graph
        self.schema = Namespace("http://schema.org/")
        self.geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        self.dbo = Namespace("http://dbpedia.org/ontology/")
        
        # Bind namespaces
        self.graph.bind("sch", self.sch)
        self.graph.bind("klm", self.klm)
        self.graph.bind("schema", self.schema)
        self.graph.bind("geo", self.geo)
        self.graph.bind("dbo", self.dbo)
        self.graph.bind("owl", OWL)
        self.graph.bind("rdfs", RDFS)
        
        logger.info("Schiphol Knowledge Graph Builder initialized")
    
    def load_data(self):
        """Load processed data files"""
        logger.info("Loading processed Schiphol data files")
        
        data = {}
        
        # Load flights data
        flights_path = os.path.join(self.processed_dir, "schiphol_flights.csv")
        if os.path.exists(flights_path):
            data["flights"] = pd.read_csv(flights_path)
            logger.info(f"Loaded {len(data['flights'])} Schiphol flights")
        else:
            logger.warning("Schiphol flights file not found")
        
        # Load enriched flights data if available
        enriched_flights_path = os.path.join(self.processed_dir, "schiphol_flights_enriched.csv")
        if os.path.exists(enriched_flights_path):
            data["enriched_flights"] = pd.read_csv(enriched_flights_path)
            logger.info(f"Loaded {len(data['enriched_flights'])} enriched Schiphol flights")
        
        # Load destinations
        destinations_path = os.path.join(self.processed_dir, "schiphol_destinations.csv")
        if os.path.exists(destinations_path):
            data["destinations"] = pd.read_csv(destinations_path)
            logger.info(f"Loaded {len(data['destinations'])} Schiphol destinations")
        else:
            logger.warning("Schiphol destinations file not found")
        
        # Load airlines
        airlines_path = os.path.join(self.processed_dir, "schiphol_airlines.csv")
        if os.path.exists(airlines_path):
            data["airlines"] = pd.read_csv(airlines_path)
            logger.info(f"Loaded {len(data['airlines'])} Schiphol airlines")
        else:
            logger.warning("Schiphol airlines file not found")
        
        # Load aircraft types
        aircraft_path = os.path.join(self.processed_dir, "schiphol_aircraft_types.csv")
        if os.path.exists(aircraft_path):
            data["aircraft_types"] = pd.read_csv(aircraft_path)
            logger.info(f"Loaded {len(data['aircraft_types'])} Schiphol aircraft types")
        else:
            logger.warning("Schiphol aircraft types file not found")
        
        return data
    
    def extend_ontology(self):
        """Extend the ontology with Schiphol-specific classes and properties"""
        logger.info("Extending ontology with Schiphol-specific concepts")
        
        # Reuse basic classes from KLM ontology for compatibility
        airport_class = self.klm.Airport
        airline_class = self.klm.Airline
        route_class = self.klm.Route
        flight_class = self.klm.Flight
        city_class = self.klm.City
        country_class = self.klm.Country
        aircraft_class = self.klm.Aircraft
        
        # Schiphol-specific classes
        schiphol_flight_class = self.sch.SchipholFlight
        self.graph.add((schiphol_flight_class, RDF.type, OWL.Class))
        self.graph.add((schiphol_flight_class, RDFS.subClassOf, flight_class))
        self.graph.add((schiphol_flight_class, RDFS.label, Literal("Schiphol Flight")))
        self.graph.add((schiphol_flight_class, RDFS.comment, Literal("A flight operated through Schiphol Airport")))
        
        # Terminal class - specific to airport operations
        terminal_class = self.sch.Terminal
        self.graph.add((terminal_class, RDF.type, OWL.Class))
        self.graph.add((terminal_class, RDFS.label, Literal("Terminal")))
        self.graph.add((terminal_class, RDFS.comment, Literal("An airport terminal")))
        
        # Gate class
        gate_class = self.sch.Gate
        self.graph.add((gate_class, RDF.type, OWL.Class))
        self.graph.add((gate_class, RDFS.label, Literal("Gate")))
        self.graph.add((gate_class, RDFS.comment, Literal("An airport gate")))
        
        # Pier class
        pier_class = self.sch.Pier
        self.graph.add((pier_class, RDF.type, OWL.Class))
        self.graph.add((pier_class, RDFS.label, Literal("Pier")))
        self.graph.add((pier_class, RDFS.comment, Literal("An airport pier")))
        
        # Define Schiphol-specific properties
        
        # Flight status properties
        flight_state_prop = self.sch.flightState
        self.graph.add((flight_state_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((flight_state_prop, RDFS.domain, schiphol_flight_class))
        self.graph.add((flight_state_prop, RDFS.range, XSD.string))
        
        # Terminal, gate, pier properties
        terminal_prop = self.sch.terminal
        self.graph.add((terminal_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((terminal_prop, RDFS.domain, schiphol_flight_class))
        self.graph.add((terminal_prop, RDFS.range, terminal_class))
        
        gate_prop = self.sch.gate
        self.graph.add((gate_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((gate_prop, RDFS.domain, schiphol_flight_class))
        self.graph.add((gate_prop, RDFS.range, gate_class))
        
        pier_prop = self.sch.pier
        self.graph.add((pier_prop, RDF.type, OWL.ObjectProperty))
        self.graph.add((pier_prop, RDFS.domain, schiphol_flight_class))
        self.graph.add((pier_prop, RDFS.range, pier_class))
        
        # European Union property (relevant for hub analysis)
        eu_prop = self.sch.isEU
        self.graph.add((eu_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((eu_prop, RDFS.domain, route_class))
        self.graph.add((eu_prop, RDFS.range, XSD.boolean))
        
        # Visa property (relevant for hub analysis)
        visa_prop = self.sch.requiresVisa
        self.graph.add((visa_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((visa_prop, RDFS.domain, route_class))
        self.graph.add((visa_prop, RDFS.range, XSD.boolean))
        
        # Flight capacity metrics
        capacity_prop = self.sch.capacity
        self.graph.add((capacity_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((capacity_prop, RDFS.domain, aircraft_class))
        self.graph.add((capacity_prop, RDFS.range, XSD.integer))
        
        # Airport operational properties - useful for hub analysis
        daily_capacity_prop = self.sch.dailyCapacity
        self.graph.add((daily_capacity_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((daily_capacity_prop, RDFS.domain, airport_class))
        self.graph.add((daily_capacity_prop, RDFS.range, XSD.integer))
        
        # Hub metrics specific to Schiphol
        connection_efficiency_prop = self.sch.connectionEfficiency
        self.graph.add((connection_efficiency_prop, RDF.type, OWL.DatatypeProperty))
        self.graph.add((connection_efficiency_prop, RDFS.domain, airport_class))
        self.graph.add((connection_efficiency_prop, RDFS.range, XSD.decimal))
        
        logger.info("Ontology extended with Schiphol-specific concepts")
    
    def add_destinations(self, destinations_df):
        """Add Schiphol destinations to the knowledge graph"""
        logger.info("Adding Schiphol destinations to the knowledge graph")
        
        destinations_added = 0
        cities_added = set()
        countries_added = set()
        
        for _, row in destinations_df.iterrows():
            # Skip if missing IATA code
            if not pd.notna(row.get("iata")):
                continue
                
            # Create airport URI
            airport_code = row["iata"]
            airport_uri = self.klm[f"airport/{airport_code}"]
            
            # Add airport triples - using klm namespace for compatibility
            self.graph.add((airport_uri, RDF.type, self.klm.Airport))
            self.graph.add((airport_uri, self.klm.code, Literal(airport_code)))
            
            # Add airport name if available
            if pd.notna(row.get("name_english")):
                self.graph.add((airport_uri, self.klm.name, Literal(row["name_english"])))
                self.graph.add((airport_uri, self.schema.name, Literal(row["name_english"])))
            
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
            
            # Add special properties for Schiphol (the central airport in this analysis)
            if airport_code == "AMS":
                self.graph.add((airport_uri, RDF.type, self.klm.HubAirport))
                self.graph.add((airport_uri, self.sch.isMainSchipholHub, Literal(True, datatype=XSD.boolean)))
                
                # Add Schiphol-specific hub metrics
                # These are placeholder values that would be calculated from actual data in a real implementation
                self.graph.add((airport_uri, self.sch.dailyCapacity, Literal(1500, datatype=XSD.integer)))  # Flights per day
                self.graph.add((airport_uri, self.sch.connectionEfficiency, Literal(0.85, datatype=XSD.decimal)))  # Connection efficiency score
            
            destinations_added += 1
        
        logger.info(f"Added {destinations_added} destinations, {len(cities_added)} cities, and {len(countries_added)} countries to the knowledge graph")
    
    def add_airlines(self, airlines_df):
        """Add Schiphol airlines to the knowledge graph"""
        logger.info("Adding Schiphol airlines to the knowledge graph")
        
        airlines_added = 0
        
        for _, row in airlines_df.iterrows():
            # Skip if missing IATA code
            if not pd.notna(row.get("iata")):
                continue
                
            # Create airline URI
            airline_code = row["iata"]
            airline_uri = self.klm[f"airline/{airline_code}"]
            
            # Add airline triples
            self.graph.add((airline_uri, RDF.type, self.klm.Airline))
            self.graph.add((airline_uri, self.klm.code, Literal(airline_code)))
            
            # Add ICAO code if available
            if pd.notna(row.get("icao")):
                self.graph.add((airline_uri, self.sch.icaoCode, Literal(row["icao"])))
            
            # Add airline name if available
            if pd.notna(row.get("public_name")):
                self.graph.add((airline_uri, self.klm.name, Literal(row["public_name"])))
                self.graph.add((airline_uri, self.schema.name, Literal(row["public_name"])))
            
            # Add special properties for KLM
            if airline_code == "KL":
                self.graph.add((airline_uri, self.klm.isCurrentProject, Literal(True, datatype=XSD.boolean)))
                # Link to Schiphol as main hub
                schiphol_uri = self.klm[f"airport/AMS"]
                self.graph.add((airline_uri, self.klm.hasHub, schiphol_uri))
            
            airlines_added += 1
        
        logger.info(f"Added {airlines_added} airlines to the knowledge graph")
    
    def add_aircraft_types(self, aircraft_types_df):
        """Add aircraft types to the knowledge graph"""
        logger.info("Adding aircraft types to the knowledge graph")
        
        aircraft_types_added = 0
        
        for _, row in aircraft_types_df.iterrows():
            # Skip if missing IATA main code
            if not pd.notna(row.get("iata_main")):
                continue
                
            # Create aircraft URI
            aircraft_code = row["iata_main"]
            aircraft_uri = self.klm[f"aircraft/{aircraft_code}"]
            
            # Add aircraft triples
            self.graph.add((aircraft_uri, RDF.type, self.klm.Aircraft))
            self.graph.add((aircraft_uri, self.klm.code, Literal(aircraft_code)))
            
            # Add aircraft name/description
            if pd.notna(row.get("long_description")):
                self.graph.add((aircraft_uri, self.klm.name, Literal(row["long_description"])))
            elif pd.notna(row.get("short_description")):
                self.graph.add((aircraft_uri, self.klm.name, Literal(row["short_description"])))
            
            # Add IATA sub-type if available
            if pd.notna(row.get("iata_sub")):
                self.graph.add((aircraft_uri, self.sch.iataSubType, Literal(row["iata_sub"])))
            
            # Add estimated capacity based on aircraft type (placeholder values)
            # In a real implementation, this would come from a more reliable source
            capacity = 0
            if "747" in aircraft_code:
                capacity = 400
            elif "777" in aircraft_code:
                capacity = 300
            elif "737" in aircraft_code:
                capacity = 150
            elif "A380" in aircraft_code:
                capacity = 500
            elif "A320" in aircraft_code:
                capacity = 180
            
            if capacity > 0:
                self.graph.add((aircraft_uri, self.sch.capacity, Literal(capacity, datatype=XSD.integer)))
            
            aircraft_types_added += 1
        
        logger.info(f"Added {aircraft_types_added} aircraft types to the knowledge graph")
    
    def add_flights(self, flights_df):
        """Add Schiphol flights to the knowledge graph"""
        logger.info("Adding Schiphol flights to the knowledge graph")
        
        flights_added = 0
        routes_added = set()
        terminal_gates = {}  # Track terminals and gates
        
        for _, row in flights_df.iterrows():
            # Skip rows without flight number or ID
            if not pd.notna(row.get("flight_number")) or not pd.notna(row.get("id")):
                continue
            
            # Create flight URI
            flight_id = row["id"]
            flight_uri = self.sch[f"flight/{flight_id}"]
            
            # Add flight triples
            self.graph.add((flight_uri, RDF.type, self.klm.Flight))
            self.graph.add((flight_uri, RDF.type, self.sch.SchipholFlight))
            self.graph.add((flight_uri, self.klm.flightNumber, Literal(row["flight_number"], datatype=XSD.string)))
            
            # Add flight name if available
            if pd.notna(row.get("flight_name")):
                self.graph.add((flight_uri, self.sch.flightName, Literal(row["flight_name"])))
            
            # Add flight date and time if available
            if pd.notna(row.get("schedule_datetime")):
                self.graph.add((flight_uri, self.klm.scheduledDeparture, Literal(row["schedule_datetime"], datatype=XSD.dateTime)))
            elif pd.notna(row.get("schedule_date")) and pd.notna(row.get("schedule_time")):
                # Combine date and time
                dt_str = f"{row['schedule_date']}T{row['schedule_time']}"
                self.graph.add((flight_uri, self.klm.scheduledDeparture, Literal(dt_str, datatype=XSD.dateTime)))
            
            # Add flight direction (arrival/departure)
            if pd.notna(row.get("flight_direction")):
                self.graph.add((flight_uri, self.sch.flightDirection, Literal(row["flight_direction"])))
            
            # Add flight states if available
            if pd.notna(row.get("flight_states")):
                states = row["flight_states"].split(", ")
                for state in states:
                    self.graph.add((flight_uri, self.sch.flightState, Literal(state)))
            
            # Add airline relationship if available
            if pd.notna(row.get("airline_code")):
                airline_uri = self.klm[f"airline/{row['airline_code']}"]
                self.graph.add((airline_uri, self.klm.operates, flight_uri))
            
            # Add terminal, gate, pier information - important for hub analysis
            if pd.notna(row.get("terminal")):
                terminal_id = row["terminal"]
                terminal_uri = self.sch[f"terminal/{terminal_id}"]
                
                # Add terminal if not already added
                if terminal_id not in terminal_gates:
                    terminal_gates[terminal_id] = set()
                    self.graph.add((terminal_uri, RDF.type, self.sch.Terminal))
                    self.graph.add((terminal_uri, self.sch.terminalNumber, Literal(terminal_id)))
                
                # Link flight to terminal
                self.graph.add((flight_uri, self.sch.terminal, terminal_uri))
            
            # Add gate information
            if pd.notna(row.get("gate")):
                gate_id = row["gate"]
                gate_uri = self.sch[f"gate/{gate_id}"]
                
                # Add gate if not already added
                if pd.notna(row.get("terminal")) and gate_id not in terminal_gates[row["terminal"]]:
                    terminal_gates[row["terminal"]].add(gate_id)
                    self.graph.add((gate_uri, RDF.type, self.sch.Gate))
                    self.graph.add((gate_uri, self.sch.gateNumber, Literal(gate_id)))
                    
                    # Link gate to terminal if available
                    if pd.notna(row.get("terminal")):
                        terminal_uri = self.sch[f"terminal/{row['terminal']}"]
                        self.graph.add((gate_uri, self.sch.inTerminal, terminal_uri))
                
                # Link flight to gate
                self.graph.add((flight_uri, self.sch.gate, gate_uri))
            
            # Add pier information
            if pd.notna(row.get("pier")):
                pier_id = row["pier"]
                pier_uri = self.sch[f"pier/{pier_id}"]
                
                # Add pier if not already processed
                self.graph.add((pier_uri, RDF.type, self.sch.Pier))
                self.graph.add((pier_uri, self.sch.pierNumber, Literal(pier_id)))
                
                # Link flight to pier
                self.graph.add((flight_uri, self.sch.pier, pier_uri))
            
            # Add route relationship and EU/visa information
            if pd.notna(row.get("destinations")):
                destinations = row["destinations"].split(", ")
                if destinations:
                    destination = destinations[0]  # Take first destination for simplicity
                    
                    # Create route URI
                    route_key = f"AMS-{destination}"  # Schiphol is always the origin or destination
                    
                    if route_key not in routes_added:
                        route_uri = self.klm[f"route/{route_key}"]
                        routes_added.add(route_key)
                        
                        # Add route triples
                        self.graph.add((route_uri, RDF.type, self.klm.Route))
                        
                        # Link to origin and destination airports
                        ams_uri = self.klm[f"airport/AMS"]
                        dest_uri = self.klm[f"airport/{destination}"]
                        
                        # Determine if it's an arrival or departure
                        if row.get("flight_direction") == "D":
                            self.graph.add((route_uri, self.klm.hasOrigin, ams_uri))
                            self.graph.add((route_uri, self.klm.hasDestination, dest_uri))
                        else:
                            self.graph.add((route_uri, self.klm.hasOrigin, dest_uri))
                            self.graph.add((route_uri, self.klm.hasDestination, ams_uri))
                        
                        # Add EU and visa information
                        if pd.notna(row.get("eu")):
                            self.graph.add((route_uri, self.sch.isEU, Literal(row["eu"] == "Y", datatype=XSD.boolean)))
                        
                        if pd.notna(row.get("visa_required")):
                            self.graph.add((route_uri, self.sch.requiresVisa, Literal(row["visa_required"] == "Y", datatype=XSD.boolean)))
                    
                    # Link flight to route
                    route_uri = self.klm[f"route/{route_key}"]
                    self.graph.add((flight_uri, self.klm.follows, route_uri))
            
            # Add aircraft information if available
            if pd.notna(row.get("aircraft_type")):
                aircraft_code = row["aircraft_type"]
                aircraft_uri = self.klm[f"aircraft/{aircraft_code}"]
                
                # Link flight to aircraft
                self.graph.add((flight_uri, self.klm.operatedWith, aircraft_uri))
            
            flights_added += 1
            
            # Add some sample flight delays for hub analysis
            # In a real implementation, this would come from actual data analysis
            if flights_added % 5 == 0:  # Add delays to 20% of flights for demonstration
                self.graph.add((flight_uri, self.klm.isDelayed, Literal(True, datatype=XSD.boolean)))
                # Add an estimated delay time in minutes (random value for demonstration)
                delay_minutes = (flights_added % 25) + 10  # Between 10 and 34 minutes
                self.graph.add((flight_uri, self.sch.delayMinutes, Literal(delay_minutes, datatype=XSD.integer)))
        
        logger.info(f"Added {flights_added} flights and {len(routes_added)} routes to the knowledge graph")
        logger.info(f"Added {len(terminal_gates)} terminals with gates to the knowledge graph")
    
    def calculate_hub_metrics(self):
        """Calculate hub metrics based on Schiphol data"""
        logger.info("Calculating Schiphol-specific hub metrics")
        
        # Count routes per destination airport (connectivity metric)
        destination_routes = {}
        dest_query = """
            SELECT ?destination
            WHERE {
                ?route klm:hasDestination ?destination .
                ?route klm:hasOrigin <http://example.org/klm/airport/AMS> .
            }
        """
        
        results = self.graph.query(dest_query)
        for row in results:
            destination = str(row.destination)
            if destination not in destination_routes:
                destination_routes[destination] = 0
            destination_routes[destination] += 1
        
        # Calculate hub potential for destination airports
        for airport_uri, route_count in destination_routes.items():
            airport = URIRef(airport_uri)
            
            # Add route count
            self.graph.add((airport, self.klm.routeCount, Literal(route_count, datatype=XSD.integer)))
            
            # Calculate a hub potential score - simple metric based on route count
            # In a real implementation, this would be more sophisticated
            hub_score = route_count * 2  # Simple multiplier
            
            # Modify score based on EU status (easier connections within EU)
            eu_query = f"""
                SELECT ?isEU
                WHERE {{
                    ?route klm:hasDestination <{airport_uri}> .
                    ?route sch:isEU ?isEU .
                }}
            """
            eu_results = list(self.graph.query(eu_query))
            
            if eu_results and eu_results[0].isEU:
                hub_score *= 1.2  # 20% bonus for EU airports
            
            # Terminal efficiency factor - based on terminal count at destination
            # Fewer terminals generally means easier connections
            terminal_query = f"""
                SELECT (COUNT(DISTINCT ?terminal) as ?terminalCount)
                WHERE {{
                    ?flight klm:follows ?route .
                    ?route klm:hasOrigin <{airport_uri}> .
                    ?flight sch:terminal ?terminal .
                }}
            """
            terminal_results = list(self.graph.query(terminal_query))
            
            if terminal_results and terminal_results[0].terminalCount:
                terminal_count = int(terminal_results[0].terminalCount)
                # Adjust score based on terminal count (more terminals = lower efficiency)
                if terminal_count == 1:
                    hub_score *= 1.1  # 10% bonus for single-terminal operations
                elif terminal_count > 3:
                    hub_score *= 0.9  # 10% penalty for complex multi-terminal operations
            
            # Add hub potential score to the airport
            self.graph.add((airport, self.sch.hubPotentialScore, Literal(hub_score, datatype=XSD.decimal)))
            
            # Add connection efficiency calculation (simplified)
            # In a real implementation, this would consider minimum connection times, etc.
            conn_efficiency = min(0.95, 0.6 + (route_count / 20.0))  # Scale between 0.6 and 0.95 based on routes
            self.graph.add((airport, self.sch.connectionEfficiency, Literal(conn_efficiency, datatype=XSD.decimal)))
            
            # If we have a high hub score, tag it as a potential hub
            if hub_score > 10:  # Arbitrary threshold for demonstration
                self.graph.add((airport, RDF.type, self.klm.PotentialHubAirport))

        logger.info(f"Added hub metrics for {len(destination_routes)} airports based on Schiphol data")
                
        # Calculate delay statistics by destination
        logger.info("Calculating delay statistics by destination")
        
        # Find all flights and their delay status
        delay_query = """
            SELECT ?flight ?destination ?isDelayed ?delayMinutes
            WHERE {
                ?flight a sch:SchipholFlight ;
                        klm:follows ?route .
                ?route klm:hasDestination ?destination .
                OPTIONAL { ?flight klm:isDelayed ?isDelayed }
                OPTIONAL { ?flight sch:delayMinutes ?delayMinutes }
            }
        """
        
        results = self.graph.query(delay_query)
        
        # Track delays by destination
        delay_data = {}
        for row in results:
            destination = str(row.destination)
            
            if destination not in delay_data:
                delay_data[destination] = {"total": 0, "delayed": 0, "delay_minutes": 0}
            
            delay_data[destination]["total"] += 1
            
            if hasattr(row, 'isDelayed') and row.isDelayed:
                delay_data[destination]["delayed"] += 1
                
                if hasattr(row, 'delayMinutes') and row.delayMinutes:
                    delay_data[destination]["delay_minutes"] += int(row.delayMinutes)
        
        # Add delay metrics to airports
        for airport_uri, stats in delay_data.items():
            if stats["total"] > 0:
                airport = URIRef(airport_uri)
                
                # Calculate delay rate
                delay_rate = stats["delayed"] / stats["total"]
                self.graph.add((airport, self.klm.delayRate, Literal(delay_rate, datatype=XSD.decimal)))
                
                # Add flight counts
                self.graph.add((airport, self.klm.totalFlights, Literal(stats["total"], datatype=XSD.integer)))
                self.graph.add((airport, self.klm.delayedFlights, Literal(stats["delayed"], datatype=XSD.integer)))
                
                # Add average delay time for delayed flights
                if stats["delayed"] > 0:
                    avg_delay = stats["delay_minutes"] / stats["delayed"]
                    self.graph.add((airport, self.sch.averageDelayMinutes, Literal(avg_delay, datatype=XSD.decimal)))
        
        logger.info(f"Added delay metrics for {len(delay_data)} airports")
    
    def generate_research_queries(self):
        """Generate sample SPARQL queries for client research questions"""
        logger.info("Generating sample SPARQL queries from Schiphol data")
        
        queries = {}
        
        # Research Question 1: Which other airport would be a suitable fit for a second hub?
        # This query integrates both Schiphol-specific and KLM data
        queries["second_hub_schiphol"] = """
            # Query to find suitable airports for a second KLM hub based on Schiphol data
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            
            SELECT ?airport ?name ?country ?routeCount ?connectionEfficiency ?isEU ?delayRate ?requiresVisa
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:routeCount ?routeCount .
                
                # Only consider airports with significant connectivity
                FILTER (?routeCount > 5)
                
                # Exclude Schiphol (current hub)
                FILTER NOT EXISTS { ?airport sch:isMainSchipholHub true }
                
                # Include Schiphol-specific metrics when available
                OPTIONAL { ?airport sch:connectionEfficiency ?connectionEfficiency }
                OPTIONAL { ?airport klm:delayRate ?delayRate }
                
                # Get EU status if available (from routes)
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:isEU ?isEU .
                }
                
                # Get visa requirements if available
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:requiresVisa ?requiresVisa .
                }
                
                # Get country information if available
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?routeCount) DESC(?connectionEfficiency)
            LIMIT 15
        """
        
        # Research Question 2: What airports are we currently not flying to but could be a good idea to expand to?
        queries["expansion_opportunities"] = """
            # Query to identify promising expansion destinations based on Schiphol connectivity
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country ?isEU ?requiresVisa
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name .
                
                # Get country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
                
                # Get EU status if available
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:isEU ?isEU .
                }
                
                # Get visa requirements if available
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:requiresVisa ?requiresVisa .
                }
                
                # Only consider airports that are NOT currently served by KLM
                FILTER NOT EXISTS {
                    ?route klm:hasDestination ?airport .
                    ?flight klm:follows ?route .
                    ?airline klm:operates ?flight .
                    ?airline klm:code "KL" .
                }
                
                # Only consider destinations already served by other airlines at Schiphol
                # (indicating market demand exists)
                EXISTS {
                    ?flight a sch:SchipholFlight .
                    ?flight klm:follows ?otherRoute .
                    ?otherRoute klm:hasDestination ?airport .
                }
            }
            ORDER BY ?country ?name
        """
        
        # Research Question 3: Correlations between specific flight destinations and delays
        queries["delay_analysis_schiphol"] = """
            # Query to analyze delay patterns by destination based on Schiphol data
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country ?delayRate ?totalFlights ?delayedFlights ?avgDelayMinutes ?isEU
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:delayRate ?delayRate ;
                        klm:totalFlights ?totalFlights ;
                        klm:delayedFlights ?delayedFlights .
                
                # Only include airports with significant flight volume
                FILTER(?totalFlights >= 5)
                
                # Get average delay minutes if available
                OPTIONAL { ?airport sch:averageDelayMinutes ?avgDelayMinutes }
                
                # Get EU status
                OPTIONAL { 
                    ?route klm:hasDestination ?airport .
                    ?route sch:isEU ?isEU .
                }
                
                # Get country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            ORDER BY DESC(?delayRate) DESC(?totalFlights)
        """
        
        # Additional query: Terminal & gate analysis for hub operations efficiency
        queries["terminal_gate_analysis"] = """
            # Query for analyzing terminal and gate usage patterns at potential hub airports
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?terminal (COUNT(?gate) as ?gateCount) (COUNT(?flight) as ?flightCount)
            WHERE {
                # Focus on airports that could be potential hubs
                ?airport a klm:PotentialHubAirport ;
                        klm:name ?name .
                
                # Find flights to this airport
                ?flight a sch:SchipholFlight ;
                        klm:follows ?route ;
                        sch:terminal ?terminal ;
                        sch:gate ?gate .
                
                ?route klm:hasDestination ?airport .
            }
            GROUP BY ?airport ?name ?terminal
            ORDER BY ?name ?terminal
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
    
    def add_eurostat_data(self):
        """
        Add sample Eurostat passenger volume data to enrich the knowledge graph
        This is a placeholder function - in a real implementation, you would load and parse actual Eurostat data
        """
        logger.info("Adding sample Eurostat passenger data to the knowledge graph")
        
        # Sample data for major European airports - annual passenger volume in millions
        # These would normally come from parsing actual Eurostat data
        passenger_volumes = {
            "AMS": 71,    # Amsterdam Schiphol
            "CDG": 76,    # Paris Charles de Gaulle
            "FRA": 70,    # Frankfurt
            "LHR": 80,    # London Heathrow
            "MAD": 61,    # Madrid
            "FCO": 48,    # Rome
            "MUC": 47,    # Munich
            "BCN": 52,    # Barcelona
            "LGW": 46,    # London Gatwick
            "ORY": 32,    # Paris Orly
            "BRU": 26,    # Brussels
            "DUB": 31,    # Dublin
            "MAN": 29,    # Manchester
            "VIE": 31,    # Vienna
            "CPH": 30,    # Copenhagen
            "HEL": 21,    # Helsinki
            "ZRH": 31,    # Zurich
            "OSL": 28,    # Oslo
            "ARN": 27,    # Stockholm
            "LIS": 31     # Lisbon
        }
        
        # Add data to knowledge graph
        for code, volume in passenger_volumes.items():
            airport_uri = self.klm[f"airport/{code}"]
            # Convert to actual passenger count
            passenger_count = volume * 1000000
            self.graph.add((airport_uri, self.klm.passengerVolume, Literal(passenger_count, datatype=XSD.integer)))
            
            # Update hub potential scores based on passenger volume
            hub_score_query = f"""
                SELECT ?hubScore
                WHERE {{
                    <{str(airport_uri)}> sch:hubPotentialScore ?hubScore .
                }}
            """
            results = list(self.graph.query(hub_score_query))
            
            if results:
                current_score = float(results[0].hubScore)
                # Adjust score based on passenger volume (normalized)
                normalized_volume = volume / 10.0  # Scale down
                new_score = current_score * (1 + normalized_volume / 15)
                
                # Update hub score
                self.graph.add((airport_uri, self.sch.hubPotentialScoreWithVolume, Literal(new_score, datatype=XSD.decimal)))
        
        logger.info(f"Added passenger volume data for {len(passenger_volumes)} major airports")
    
    def build_knowledge_graph(self):
        """Build the knowledge graph from processed Schiphol data"""
        logger.info("Building knowledge graph from Schiphol data for hub expansion analysis")
        
        # Extend the ontology with Schiphol-specific classes and properties
        self.extend_ontology()
        
        # Load data
        data = self.load_data()
        
        # Add destinations (airports, cities, countries)
        if "destinations" in data:
            self.add_destinations(data["destinations"])
        
        # Add airlines
        if "airlines" in data:
            self.add_airlines(data["airlines"])
        
        # Add aircraft types
        if "aircraft_types" in data:
            self.add_aircraft_types(data["aircraft_types"])
        
        # Add flights (use enriched flights if available, otherwise use regular flights)
        if "enriched_flights" in data:
            self.add_flights(data["enriched_flights"])
        elif "flights" in data:
            self.add_flights(data["flights"])
        
        # Calculate hub metrics based on the Schiphol data
        self.calculate_hub_metrics()
        
        # Add sample Eurostat data
        self.add_eurostat_data()
        
        # Save knowledge graph
        timestamp = datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(self.output_dir, f"schiphol_hub_expansion_kg_{timestamp}.ttl")
        self.graph.serialize(destination=output_path, format="turtle")
        
        logger.info(f"Schiphol knowledge graph saved to {output_path}")
        
        # Also save as RDF/XML for compatibility with some tools
        xml_output_path = os.path.join(self.output_dir, f"schiphol_hub_expansion_kg_{timestamp}.rdf")
        self.graph.serialize(destination=xml_output_path, format="xml")
        
        logger.info(f"Schiphol knowledge graph also saved as RDF/XML to {xml_output_path}")
        
        # Generate sample queries for client research questions
        self.generate_research_queries()
        
        # Return stats
        return {
            "triples": len(self.graph),
            "file_path": output_path
        }

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Build a knowledge graph from Schiphol API data")
    parser.add_argument("--processed-dir", default="data/Schiphol/processed", 
                        help="Directory containing processed Schiphol data files")
    parser.add_argument("--output-dir", default="data/Schiphol/knowledge_graph",
                        help="Directory to save the knowledge graph files")
    
    args = parser.parse_args()
    
    # Create knowledge graph builder
    builder = SchipholKnowledgeGraphBuilder(processed_dir=args.processed_dir, output_dir=args.output_dir)
    
    # Build knowledge graph
    stats = builder.build_knowledge_graph()
    
    logger.info(f"Schiphol knowledge graph built with {stats['triples']} triples")
    logger.info(f"Knowledge graph files saved to {args.output_dir}")
    logger.info("The knowledge graph can now be used to analyze KLM's hub expansion opportunities")

if __name__ == "__main__":
    main()