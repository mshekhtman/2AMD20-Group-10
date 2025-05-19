# 2AMD20-Group-10```markdown
# KLM Hub Expansion Analysis Project

## Overview

This project analyzes flight data from KLM Royal Dutch Airlines to support their strategic decision-making for multi-hub expansion. Using knowledge graph technology, we integrate data from multiple sources to recommend optimal locations for a second hub, identify potential new flight destinations, and analyze delays across KLM's network.

## Project Structure

```
2AMD20-GROUP-10/
│
├── config/                  # Configuration files
│   ├── klm_api.json         # KLM API credentials and settings
│   └── schiphol_api.json    # Schiphol API credentials and settings
│
├── data/                    # Data directory
│   ├── KLM/                 # KLM API data
│   │   ├── raw/             # Raw data from KLM API
│   │   │   ├── endpoint_test_results_20250519_184031.json
│   │   │   ├── klm_collection_summary_20250519_191723.json
│   │   │   ├── klm_flightstatus_response_20250519_191719.json
│   │   │   └── working_endpoints_20250519_184031.json
│   │   │
│   │   └── processed/       # Processed data files (CSV)
│   │       ├── airports.csv
│   │       └── flights.csv
│   │
│   ├── Schiphol/            # Schiphol API data
│   │   ├── raw/             # Raw data from Schiphol API
│   │   │   ├── aircraft_types_20250519_183703.json
│   │   │   ├── airlines_20250519_183701.json
│   │   │   ├── all_flights_all_20250519_183653.json
│   │   │   ├── destinations_20250519_183659.json
│   │   │   ├── flights_all_page0_20250519_183645.json
│   │   │   ├── flights_all_page1_20250519_183647.json
│   │   │   └── flights_all_page4_20250519_183652.json
│   │   │
│   │   └── processed/       # Processed Schiphol data
│   │       ├── schiphol_aircraft_types.csv
│   │       ├── schiphol_airlines.csv
│   │       ├── schiphol_destinations.csv
│   │       └── schiphol_flights.csv
│   │
│   └── knowledge_graph/     # Knowledge graph files
│       ├── klm_hub_expansion_kg_20250519.rdf
│       ├── klm_hub_expansion_kg_20250519.ttl
│       └── queries/         # SPARQL query files
│           ├── hub_analysis_query.sparql
│           ├── route_expansion_query.sparql
│           └── delay_analysis_query.sparql
│
├── scripts/                 # Python scripts
│   ├── KLM/                 # KLM-related scripts
│   │   ├── flight_processor.py        # Flight data processing utilities
│   │   ├── kg_builder.py              # Builds knowledge graph from data
│   │   ├── klm_collector.py           # Collects data from KLM API
│   │   ├── klm_flight_status_example.py  # Example script for API
│   │   ├── klm_processor.py           # Processes KLM data
│   │   └── klm_run_pipeline.py        # End-to-end pipeline script
│   │
│   └── Schiphol/            # Schiphol-related scripts
│       ├── sch_api_test.py            # Tests Schiphol API connection
│       ├── sch_collector.py           # Collects data from Schiphol API
│       ├── sch_processor.py           # Processes Schiphol data
│       ├── sch_quickstart.py          # Quick start example
│       └── sch_run_pipeline.py        # Pipeline script for Schiphol
│
├── venv/                    # Virtual environment (not in version control)
│   ├── Include/             # Environment include files
│   ├── Lib/                 # Python libraries
│   └── Scripts/             # Activation scripts
│
├── Airports28062017_18927823887324918.csv  # External airports dataset
├── kg_builder.log                          # Knowledge graph builder logs
├── klm_data_collection.log                 # Data collection logs
├── README.md                               # Project documentation
└── requirements.txt                        # Python dependencies
```

## Research Questions

This project addresses the following research questions for KLM:

1. **Hub Selection**: Which other airport would be a suitable fit to be a second hub for KLM?
2. **Route Expansion**: What airports is KLM currently not flying to but could be good expansion opportunities?
3. **Delay Analysis**: Can we find significant correlations between specific flight destinations and delays?

## Data Sources

- **KLM API**: Flight status, schedules, routes, aircraft, operational metrics  
- **Schiphol API**: Flight operations at KLM’s primary hub  
- **External Datasets**: Eurostat, global airports DB, historical weather

## Technology Stack

- Python 3.x, RDFLib, Pandas, GraphDB, SPARQL

## Setup Instructions

### Install

```bash
git clone https://github.com/yourusername/2AMD20-GROUP-10.git
cd 2AMD20-GROUP-10
python -m venv venv
# Windows
.\venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
pip install -r requirements.txt
```

### Configure API Keys

Ensure `config/klm_api.json` and `config/schiphol_api.json` contain valid credentials.

### Data Collection

```bash
python scripts/KLM/klm_collector.py
python scripts/Schiphol/sch_collector.py
```

### Processing

```bash
python scripts/KLM/klm_processor.py
python scripts/Schiphol/sch_processor.py
```

### Knowledge Graph

```bash
python scripts/KLM/kg_builder.py
```

### Pipeline Execution

```bash
python scripts/KLM/klm_run_pipeline.py
```

## Knowledge Graph Structure

**Classes**: `Airport`, `Airline`, `Flight`, `Route`, `City`, `Country`  
**Key Properties**: `hubPotentialScore`, `passengerVolume`, `delayRate`, `routeCount`, `follows`, `operates`, `hasOrigin`, `hasDestination`

## Analysis Methods

### Hub Potential

- Connectivity Index
- Distance from AMS
- Passenger volumes
- Delay rates
- Market relevance

### Route Expansion

- Passenger demand
- Population metrics
- Competitive gaps
- Network synergy

### Delay Correlation

- Destination-based delays
- Seasonal trends
- Causal drivers
- Significance testing

## Contributors

- Mark Shekhtman Prishchenko
- Mees Peter 
- Justin Habets

## License

Educational use only – 2AMD20 Knowledge Engineering (TU Eindhoven)

## Acknowledgments

- Air France-KLM Open Data  
- Schiphol API  
- TU/e Database Group
```
