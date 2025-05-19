"""
KLM Knowledge Graph Pipeline Runner

This script runs the entire KLM knowledge graph pipeline:
1. Collect flight data from KLM API
2. Process the data into structured formats
3. Build a knowledge graph
"""

import os
import logging
import argparse
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Pipeline")

def create_directory_structure():
    """Create directory structure for the project"""
    logger.info("Creating directory structure")
    
    directories = [
        "data/raw/klm_api",
        "data/processed",
        "data/knowledge_graph"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    logger.info("Directory structure created")

def run_data_collection(api_key):
    """Run KLM API data collection"""
    logger.info("Running KLM API data collection")
    
    from scripts.klm_collector import KLMApiCollector
    
    collector = KLMApiCollector(api_key)
    
    # Test endpoints first
    working_endpoints = collector.test_all_endpoints()
    
    # Collect flight status data
    if "/opendata/flightstatus" in working_endpoints:
        collector.collect_flight_status()
    else:
        logger.warning("Flight status endpoint not working, skipping collection")
    
    logger.info("KLM API data collection completed")

def run_data_processing():
    """Run flight data processing"""
    logger.info("Running flight data processing")
    
    from scripts.flight_processor import FlightDataProcessor
    
    processor = FlightDataProcessor()
    processor.process_flights()
    
    logger.info("Flight data processing completed")

def run_knowledge_graph_building():
    """Run knowledge graph building"""
    logger.info("Running knowledge graph building")
    
    from scripts.kg_builder import KGBuilder
    
    builder = KGBuilder()
    stats = builder.build_knowledge_graph()
    
    logger.info(f"Knowledge graph building completed with {stats['triples']} triples")
    
    return stats

def run_full_pipeline(api_key):
    """Run the full knowledge graph pipeline"""
    logger.info("Starting full knowledge graph pipeline")
    
    start_time = time.time()
    
    # Create directory structure
    create_directory_structure()
    
    # Run data collection
    run_data_collection(api_key)
    
    # Run data processing
    run_data_processing()
    
    # Run knowledge graph building
    stats = run_knowledge_graph_building()
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    logger.info(f"Full pipeline completed in {elapsed_time:.2f} seconds")
    
    return stats

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Run KLM knowledge graph pipeline")
    parser.add_argument("--api-key", default="qawc94kkpwnkcmch3vgc4cdm", help="KLM API key")
    parser.add_argument("--step", choices=["all", "collect", "process", "build"], 
                        default="all", help="Pipeline step to run")
    
    args = parser.parse_args()
    
    try:
        # Run the requested step
        if args.step == "all":
            run_full_pipeline(args.api_key)
        elif args.step == "collect":
            create_directory_structure()
            run_data_collection(args.api_key)
        elif args.step == "process":
            create_directory_structure()
            run_data_processing()
        elif args.step == "build":
            create_directory_structure()
            run_knowledge_graph_building()
        
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())