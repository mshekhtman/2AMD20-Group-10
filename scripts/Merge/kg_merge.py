"""
KLM-Schiphol Knowledge Graph Merger

This script merges the separate KLM and Schiphol knowledge graphs into a unified graph
for comprehensive analysis of KLM's hub expansion opportunities.
"""

import os
import logging
from rdflib import Graph, Namespace
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kg_merger.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("KGMerger")

class KnowledgeGraphMerger:
    """Tool for merging KLM and Schiphol knowledge graphs"""
    
    def __init__(self, kg_dir='data/knowledge_graph', output_dir='data/knowledge_graph'):
        """Initialize the knowledge graph merger"""
        self.kg_dir = kg_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Define namespaces for logging
        self.klm = Namespace("http://example.org/klm/")
        self.sch = Namespace("http://example.org/schiphol/")
        
        logger.info("Knowledge Graph Merger initialized")
    
    def find_latest_kg_files(self):
        """Find the latest KLM and Schiphol knowledge graph files"""
        logger.info("Finding latest knowledge graph files")
        
        # Find latest KLM knowledge graph
        klm_files = [f for f in os.listdir(self.kg_dir) if f.startswith("klm_hub_expansion_kg_") and f.endswith(".ttl")]
        if not klm_files:
            logger.warning("No KLM knowledge graph files found")
            return None, None
        
        latest_klm_file = max(klm_files)
        klm_path = os.path.join(self.kg_dir, latest_klm_file)
        logger.info(f"Latest KLM knowledge graph: {klm_path}")
        
        # Find latest Schiphol knowledge graph
        sch_files = [f for f in os.listdir(self.kg_dir) if f.startswith("schiphol_hub_expansion_kg_") and f.endswith(".ttl")]
        if not sch_files:
            logger.warning("No Schiphol knowledge graph files found")
            return klm_path, None
        
        latest_sch_file = max(sch_files)
        sch_path = os.path.join(self.kg_dir, latest_sch_file)
        logger.info(f"Latest Schiphol knowledge graph: {sch_path}")
        
        return klm_path, sch_path
    
    def merge_graphs(self, klm_path, sch_path):
        """Merge the KLM and Schiphol knowledge graphs"""
        if not klm_path and not sch_path:
            logger.error("No knowledge graph files to merge")
            return None
        
        # Create merged graph
        merged_graph = Graph()
        
        # Load KLM graph if available
        if klm_path:
            logger.info(f"Loading KLM knowledge graph from {klm_path}")
            klm_graph = Graph()
            klm_graph.parse(klm_path, format="turtle")
            logger.info(f"KLM knowledge graph loaded with {len(klm_graph)} triples")
            
            # Add all triples to merged graph
            for s, p, o in klm_graph:
                merged_graph.add((s, p, o))
        
        # Load Schiphol graph if available
        if sch_path:
            logger.info(f"Loading Schiphol knowledge graph from {sch_path}")
            sch_graph = Graph()
            sch_graph.parse(sch_path, format="turtle")
            logger.info(f"Schiphol knowledge graph loaded with {len(sch_graph)} triples")
            
            # Add all triples to merged graph
            for s, p, o in sch_graph:
                merged_graph.add((s, p, o))
        
        logger.info(f"Merged knowledge graph created with {len(merged_graph)} triples")
        return merged_graph
    
    def generate_unified_queries(self, graph):
        """Generate unified SPARQL queries that leverage both KLM and Schiphol data"""
        logger.info("Generating unified SPARQL queries for comprehensive analysis")
        
        queries = {}
        
        # Comprehensive second hub analysis combining KLM and Schiphol metrics
        queries["comprehensive_hub_analysis"] = """
            # Comprehensive analysis for second hub selection combining all metrics
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            
            SELECT ?airport ?name ?country 
                   ?routeCount ?passengerVolume 
                   ?connectionEfficiency ?delayRate 
                   ?isEU ?hubPotentialScore
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name .
                
                # We need both route count and passenger volume for a good analysis
                ?airport klm:routeCount ?routeCount ;
                         klm:passengerVolume ?passengerVolume .
                
                # Filter out current hubs
                FILTER NOT EXISTS { ?airport klm:isMainHub true }
                FILTER NOT EXISTS { ?airport sch:isMainSchipholHub true }
                
                # Only consider significant airports
                FILTER(?routeCount >= 5)
                FILTER(?passengerVolume >= 10000000)
                
                # Include hub metrics from both KLM and Schiphol data
                OPTIONAL { ?airport klm:hubPotentialScore ?hubPotentialScore }
                OPTIONAL { ?airport sch:hubPotentialScore ?schHubScore }
                OPTIONAL { ?airport sch:hubPotentialScoreWithVolume ?schVolHubScore }
                OPTIONAL { ?airport klm:delayRate ?delayRate }
                OPTIONAL { ?airport sch:connectionEfficiency ?connectionEfficiency }
                
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
                
                # Combine hub scores from both sources if available
                BIND(
                    IF(BOUND(?hubPotentialScore), 
                       IF(BOUND(?schVolHubScore), 
                          (?hubPotentialScore + ?schVolHubScore) / 2, 
                          ?hubPotentialScore),
                       IF(BOUND(?schVolHubScore), 
                          ?schVolHubScore, 
                          IF(BOUND(?schHubScore), 
                             ?schHubScore, 
                             ?routeCount)))
                    AS ?hubPotentialScore)
            }
            ORDER BY DESC(?hubPotentialScore)
            LIMIT 10
        """
        
        # Delay analysis integrating both KLM and Schiphol data
        queries["comprehensive_delay_analysis"] = """
            # Comprehensive delay analysis combining KLM and Schiphol data
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country 
                   ?delayRate ?totalFlights ?delayedFlights 
                   ?avgDelayMinutes ?isEU ?terminal ?gate
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:delayRate ?delayRate ;
                        klm:totalFlights ?totalFlights ;
                        klm:delayedFlights ?delayedFlights .
                
                # Only include airports with significant flight volume
                FILTER(?totalFlights >= 10)
                FILTER(?delayRate > 0.1)  # More than 10% delays
                
                # Get average delay minutes if available
                OPTIONAL { ?airport sch:averageDelayMinutes ?avgDelayMinutes }
                
                # Get terminal and gate information for further analysis
                OPTIONAL {
                    ?flight a sch:SchipholFlight ;
                            klm:follows ?route ;
                            sch:terminal ?terminal ;
                            sch:gate ?gate .
                    ?route klm:hasDestination ?airport .
                }
                
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
        
        # Route expansion opportunities with comprehensive metrics
        queries["comprehensive_expansion_opportunities"] = """
            # Comprehensive route expansion analysis
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country 
                   ?passengerVolume ?isEU ?requiresVisa 
                   (GROUP_CONCAT(DISTINCT ?airline; SEPARATOR=', ') AS ?airlines)
            WHERE {
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:passengerVolume ?passengerVolume .
                
                # Filter for significant passenger volume
                FILTER(?passengerVolume > 5000000)
                
                # Only consider airports not currently served by KLM
                FILTER NOT EXISTS {
                    ?klmRoute klm:hasDestination ?airport .
                    ?klmFlight klm:follows ?klmRoute .
                    ?klmAirline klm:operates ?klmFlight .
                    ?klmAirline klm:code "KL" .
                }
                
                # Only consider destinations already served by other airlines at Schiphol
                ?flight a sch:SchipholFlight ;
                        klm:follows ?otherRoute .
                ?otherRoute klm:hasDestination ?airport .
                ?otherAirline klm:operates ?flight ;
                              klm:name ?airline .
                
                # Get EU status if available
                OPTIONAL { 
                    ?otherRoute sch:isEU ?isEU .
                }
                
                # Get visa requirements if available
                OPTIONAL { 
                    ?otherRoute sch:requiresVisa ?requiresVisa .
                }
                
                # Get country information
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
        
        # Terminal and operational efficiency analysis for hub selection
        queries["terminal_operational_efficiency"] = """
            # Terminal and operational efficiency analysis for hub selection
            PREFIX klm: <http://example.org/klm/>
            PREFIX sch: <http://example.org/schiphol/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?airport ?name ?country 
                   (COUNT(DISTINCT ?terminal) AS ?terminalCount)
                   (COUNT(DISTINCT ?pier) AS ?pierCount)
                   (AVG(?connectionEfficiency) AS ?avgConnectionEfficiency)
                   ?routeCount ?passengerVolume
            WHERE {
                # Potential hub airports with significant metrics
                ?airport a klm:Airport ;
                        klm:name ?name ;
                        klm:routeCount ?routeCount ;
                        klm:passengerVolume ?passengerVolume .
                
                # Only consider significant airports
                FILTER(?routeCount >= 10)
                FILTER(?passengerVolume >= 10000000)
                
                # Get terminal and pier information
                OPTIONAL {
                    ?flight a sch:SchipholFlight ;
                            klm:follows ?route ;
                            sch:terminal ?terminal ;
                            sch:pier ?pier .
                    ?route klm:hasDestination ?airport .
                }
                
                # Get connection efficiency
                OPTIONAL { ?airport sch:connectionEfficiency ?connectionEfficiency }
                
                # Get country information
                OPTIONAL { 
                    ?airport klm:locatedIn ?city .
                    ?city klm:locatedIn ?countryUri .
                    ?countryUri klm:name ?country .
                }
            }
            GROUP BY ?airport ?name ?country ?routeCount ?passengerVolume
            ORDER BY DESC(?avgConnectionEfficiency) ASC(?terminalCount)
            LIMIT 10
        """
        
        # Save queries to files
        queries_dir = os.path.join(self.output_dir, "queries")
        os.makedirs(queries_dir, exist_ok=True)
        
        for name, query in queries.items():
            query_path = os.path.join(queries_dir, f"{name}_query.sparql")
            with open(query_path, 'w', encoding='utf-8') as f:
                f.write(query)
            logger.info(f"Saved unified query '{name}' to {query_path}")
        
        return queries
    
    def merge_knowledge_graphs(self):
        """Merge the KLM and Schiphol knowledge graphs and save as a unified graph"""
        logger.info("Starting knowledge graph merge process")
        
        # Find latest KG files
        klm_path, sch_path = self.find_latest_kg_files()
        
        # Merge graphs
        merged_graph = self.merge_graphs(klm_path, sch_path)
        if not merged_graph:
            logger.error("Failed to create merged graph")
            return None
        
        # Save merged graph
        timestamp = datetime.now().strftime("%Y%m%d")
        output_path = os.path.join(self.output_dir, f"unified_hub_expansion_kg_{timestamp}.ttl")
        merged_graph.serialize(destination=output_path, format="turtle")
        
        logger.info(f"Unified knowledge graph saved to {output_path}")
        
        # Also save as RDF/XML for compatibility
        xml_output_path = os.path.join(self.output_dir, f"unified_hub_expansion_kg_{timestamp}.rdf")
        merged_graph.serialize(destination=xml_output_path, format="xml")
        
        logger.info(f"Unified knowledge graph also saved as RDF/XML to {xml_output_path}")
        
        # Generate unified queries
        self.generate_unified_queries(merged_graph)
        
        # Return stats
        return {
            "triples": len(merged_graph),
            "file_path": output_path
        }

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge KLM and Schiphol knowledge graphs")
    parser.add_argument("--kg-dir", default="data/knowledge_graph", 
                        help="Directory containing knowledge graph files")
    parser.add_argument("--output-dir", default="data/knowledge_graph",
                        help="Directory to save the merged knowledge graph")
    
    args = parser.parse_args()
    
    # Create knowledge graph merger
    merger = KnowledgeGraphMerger(kg_dir=args.kg_dir, output_dir=args.output_dir)
    
    # Merge knowledge graphs
    stats = merger.merge_knowledge_graphs()
    
    if stats:
        logger.info(f"Unified knowledge graph created with {stats['triples']} triples")
        logger.info(f"Unified knowledge graph saved to {args.output_dir}")
        logger.info("The unified knowledge graph can now be used for comprehensive hub expansion analysis")
    else:
        logger.error("Failed to create unified knowledge graph")

if __name__ == "__main__":
    main()