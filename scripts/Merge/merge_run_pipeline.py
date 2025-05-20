"""
KLM Hub Expansion Analysis Pipeline

This script runs the entire data processing pipeline:
1. Collect data from both KLM and Schiphol APIs
2. Process the data into structured formats
3. Build separate knowledge graphs for KLM and Schiphol data
4. Merge the knowledge graphs for comprehensive analysis
"""

import os
import logging
import argparse
import time
import subprocess
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hub_expansion_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("HubExpansionPipeline")

def create_directory_structure():
    """Create directory structure for the project"""
    logger.info("Creating directory structure")
    
    directories = [
        "data/KLM/raw",
        "data/KLM/processed",
        "data/Schiphol/raw",
        "data/Schiphol/processed",
        "data/knowledge_graph",
        "data/knowledge_graph/queries"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    logger.info("Directory structure created")

def run_data_collection():
    """Run data collection from KLM and Schiphol APIs"""
    logger.info("Running data collection from KLM and Schiphol APIs")
    
    # Collect KLM data
    logger.info("Collecting KLM API data")
    try:
        subprocess.run(["python", "scripts/KLM/klm_collector.py", "--endpoint", "all"], check=True)
        logger.info("KLM data collection completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error collecting KLM data: {str(e)}")
    
    # Collect Schiphol data
    logger.info("Collecting Schiphol API data")
    try:
        subprocess.run(["python", "scripts/Schiphol/sch_collector.py", "--endpoint", "all"], check=True)
        logger.info("Schiphol data collection completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error collecting Schiphol data: {str(e)}")

def run_data_processing():
    """Run data processing for KLM and Schiphol data"""
    logger.info("Running data processing")
    
    # Process KLM data
    logger.info("Processing KLM data")
    try:
        subprocess.run(["python", "scripts/KLM/klm_processor.py", "--type", "all"], check=True)
        logger.info("KLM data processing completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error processing KLM data: {str(e)}")
    
    # Process Schiphol data
    logger.info("Processing Schiphol data")
    try:
        subprocess.run(["python", "scripts/Schiphol/sch_processor.py", "--type", "all"], check=True)
        logger.info("Schiphol data processing completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error processing Schiphol data: {str(e)}")

def run_knowledge_graph_building():
    """Run knowledge graph building for KLM and Schiphol data"""
    logger.info("Running knowledge graph building")
    
    # Build KLM knowledge graph
    logger.info("Building KLM knowledge graph")
    try:
        subprocess.run(["python", "scripts/KLM/kg_builder.py"], check=True)
        logger.info("KLM knowledge graph building completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error building KLM knowledge graph: {str(e)}")
    
    # Build Schiphol knowledge graph
    logger.info("Building Schiphol knowledge graph")
    try:
        subprocess.run(["python", "scripts/Schiphol/sch_kg_builder.py"], check=True)
        logger.info("Schiphol knowledge graph building completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error building Schiphol knowledge graph: {str(e)}")

def run_knowledge_graph_merger():
    """Run knowledge graph merger"""
    logger.info("Running knowledge graph merger")
    
    try:
        subprocess.run(["python", "scripts/kg_merger.py"], check=True)
        logger.info("Knowledge graph merger completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error merging knowledge graphs: {str(e)}")

def run_full_pipeline():
    """Run the full hub expansion analysis pipeline"""
    logger.info("Starting full hub expansion analysis pipeline")
    
    start_time = time.time()
    
    # Create directory structure
    create_directory_structure()
    
    # Run data collection
    run_data_collection()
    
    # Run data processing
    run_data_processing()
    
    # Run knowledge graph building
    run_knowledge_graph_building()
    
    # Run knowledge graph merger
    run_knowledge_graph_merger()
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    logger.info(f"Full pipeline completed in {elapsed_time:.2f} seconds")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Run KLM hub expansion analysis pipeline")
    parser.add_argument("--step", choices=["all", "collect", "process", "build", "merge"], 
                        default="all", help="Pipeline step to run")
    
    args = parser.parse_args()
    
    try:
        # Run the requested step
        if args.step == "all":
            run_full_pipeline()
        elif args.step == "collect":
            create_directory_structure()
            run_data_collection()
        elif args.step == "process":
            run_data_processing()
        elif args.step == "build":
            run_knowledge_graph_building()
        elif args.step == "merge":
            run_knowledge_graph_merger()
        
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())