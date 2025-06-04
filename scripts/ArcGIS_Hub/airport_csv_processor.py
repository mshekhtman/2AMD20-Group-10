"""
Airport CSV Data Processor for KLM Hub Analysis

This script processes the ArcGIS Hub airports dataset to extract and clean
airport data relevant for hub expansion analysis.

python scripts\ArcGIS_Hub\airport_csv_processor.py Airports28062017_189278238873247918.csv
"""

import pandas as pd
import numpy as np
import os
import logging
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AirportProcessor")

class AirportCSVProcessor:
    def __init__(self, csv_file_path: str, output_dir: str = 'data/ArcGIS_Hub'):
        """Initialize the airport CSV processor"""
        self.csv_file_path = csv_file_path
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info("Airport CSV Processor initialized")
    
    def load_raw_data(self) -> pd.DataFrame:
        """Load and initial cleaning of raw CSV data"""
        logger.info(f"Loading airport data from {self.csv_file_path}")
        
        try:
            # Read CSV with error handling
            df = pd.read_csv(self.csv_file_path, encoding='utf-8')
            logger.info(f"Loaded {len(df)} airports from CSV")
            
            # Basic data info
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Airport types: {df['type'].value_counts().head()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
            raise
    
    def clean_and_filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and filter airport data for hub analysis"""
        logger.info("Cleaning and filtering airport data...")
        
        original_count = len(df)
        
        # 1. Clean problematic characters and fields
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace('**', '', regex=False)
                df[col] = df[col].str.strip()
                df[col] = df[col].replace('', np.nan)
        
        # 2. Filter for relevant airport types (exclude small private airfields)
        relevant_types = [
            'large_airport',
            'medium_airport', 
            'small_airport'  # Keep some small airports that might have scheduled service
        ]
        
        df_filtered = df[df['type'].isin(relevant_types)].copy()
        logger.info(f"After type filtering: {len(df_filtered)} airports ({original_count - len(df_filtered)} removed)")
        
        # 3. Focus on airports with IATA codes (commercial airports)
        df_filtered = df_filtered[df_filtered['iata_code'].notna() & (df_filtered['iata_code'] != 'nan')].copy()
        logger.info(f"After IATA filtering: {len(df_filtered)} airports")
        
        # 4. Focus on airports with scheduled service
        df_filtered = df_filtered[df_filtered['scheduled_service'].str.lower() == 'yes'].copy()
        logger.info(f"After scheduled service filtering: {len(df_filtered)} airports")
        
        # 5. Geographic filtering - focus on Europe and major global hubs
        # Europe: Focus on EU countries + UK, Norway, Switzerland
        european_countries = [
            'GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'AT', 'CH', 'NO', 'SE', 'DK',
            'FI', 'PL', 'CZ', 'HU', 'PT', 'GR', 'IE', 'SK', 'SI', 'HR', 'BG', 'RO',
            'LT', 'LV', 'EE', 'LU', 'MT', 'CY'
        ]
        
        # Major global hubs for comparison
        major_hubs = [
            'US', 'CA', 'AE', 'SG', 'HK', 'JP', 'KR', 'CN', 'IN', 'AU', 'TR', 'EG', 'ZA', 'BR', 'MX'
        ]
        
        relevant_countries = european_countries + major_hubs
        df_filtered = df_filtered[df_filtered['iso_country'].isin(relevant_countries)].copy()
        logger.info(f"After geographic filtering: {len(df_filtered)} airports")
        
        # 6. Data quality checks
        # Remove airports without coordinates
        df_filtered = df_filtered.dropna(subset=['latitude_deg', 'longitude_deg'])
        
        # Remove airports with invalid coordinates
        df_filtered = df_filtered[
            (df_filtered['latitude_deg'].between(-90, 90)) &
            (df_filtered['longitude_deg'].between(-180, 180))
        ]
        
        logger.info(f"After coordinate validation: {len(df_filtered)} airports")
        
        return df_filtered
    
    def enrich_airport_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich airport data with additional fields for hub analysis"""
        logger.info("Enriching airport data for hub analysis...")
        
        df_enriched = df.copy()
        
        # 1. Standardize airport codes
        df_enriched['iata_code'] = df_enriched['iata_code'].str.upper().str.strip()
        
        # 2. Create standardized names
        df_enriched['standard_name'] = df_enriched['name'].str.replace(' Airport', '').str.replace(' International', '')
        
        # 3. Add hub potential indicators based on infrastructure
        
        # Runway capacity indicator
        df_enriched['runway_length_m'] = df_enriched['runway_length_ft'] * 0.3048  # Convert to meters
        df_enriched['runway_width_m'] = df_enriched['runway_width_ft'] * 0.3048
        
        # Classify runway capacity
        def classify_runway_capacity(length_m):
            if pd.isna(length_m):
                return 'unknown'
            elif length_m >= 3000:
                return 'large'  # Can handle wide-body aircraft
            elif length_m >= 2000:
                return 'medium'  # Regional/narrow-body
            else:
                return 'small'   # Limited capacity
        
        df_enriched['runway_capacity'] = df_enriched['runway_length_m'].apply(classify_runway_capacity)
        
        # 4. Geographic regions for analysis
        continent_mapping = {
            'EU': 'Europe',
            'NA': 'North America', 
            'AS': 'Asia',
            'AF': 'Africa',
            'SA': 'South America',
            'OC': 'Oceania'
        }
        df_enriched['continent_name'] = df_enriched['continent'].map(continent_mapping)
        
        # 5. Priority scoring for hub analysis
        def calculate_airport_priority(row):
            score = 0
            
            # Airport type score
            if row['type'] == 'large_airport':
                score += 3
            elif row['type'] == 'medium_airport':
                score += 2
            else:
                score += 1
            
            # Runway capacity score
            if row['runway_capacity'] == 'large':
                score += 3
            elif row['runway_capacity'] == 'medium':
                score += 2
            elif row['runway_capacity'] == 'small':
                score += 1
            
            # Geographic priority (Europe gets bonus for KLM)
            if row['continent'] == 'EU':
                score += 2
            elif row['iso_country'] in ['US', 'CA', 'AE', 'SG', 'HK']:  # Major hub countries
                score += 1
            
            return score
        
        df_enriched['hub_priority_score'] = df_enriched.apply(calculate_airport_priority, axis=1)
        
        # 6. Add distance from Amsterdam (current KLM hub) for strategic analysis
        ams_lat, ams_lon = 52.3676, 4.9041  # Amsterdam Schiphol coordinates
        
        def calculate_distance_km(lat1, lon1, lat2, lon2):
            """Calculate distance between two points using Haversine formula"""
            from math import radians, cos, sin, asin, sqrt
            
            # Convert to radians
            lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
            
            # Haversine formula
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * asin(sqrt(a))
            r = 6371  # Radius of earth in kilometers
            
            return c * r
        
        df_enriched['distance_from_ams_km'] = df_enriched.apply(
            lambda row: calculate_distance_km(ams_lat, ams_lon, row['latitude_deg'], row['longitude_deg']),
            axis=1
        )
        
        # 7. Strategic distance categories
        def classify_strategic_distance(distance_km):
            if distance_km < 500:
                return 'regional'      # Regional hub potential
            elif distance_km < 2000:
                return 'continental'   # Continental hub potential  
            elif distance_km < 8000:
                return 'intercontinental'  # Long-haul hub potential
            else:
                return 'global'        # Global reach
        
        df_enriched['strategic_distance'] = df_enriched['distance_from_ams_km'].apply(classify_strategic_distance)
        
        logger.info("Airport data enrichment completed")
        return df_enriched
    
    def create_hub_candidates_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create focused dataset of hub candidates"""
        logger.info("Creating hub candidates dataset...")
        
        # Select key columns for hub analysis
        hub_columns = [
            'iata_code', 'name', 'standard_name', 'type',
            'latitude_deg', 'longitude_deg', 'elevation_ft',
            'continent_name', 'iso_country', 'iso_region', 'municipality',
            'runway_length_m', 'runway_width_m', 'runway_capacity',
            'hub_priority_score', 'distance_from_ams_km', 'strategic_distance'
        ]
        
        # Filter for high-priority candidates
        hub_candidates = df[
            (df['hub_priority_score'] >= 4) |  # High priority airports
            (df['type'] == 'large_airport') |   # All large airports
            ((df['type'] == 'medium_airport') & (df['runway_capacity'] == 'large'))  # Large-runway medium airports
        ].copy()
        
        # Select and rename columns
        hub_candidates = hub_candidates[hub_columns].copy()
        
        # Sort by priority score and distance
        hub_candidates = hub_candidates.sort_values(['hub_priority_score', 'distance_from_ams_km'], 
                                                   ascending=[False, True])
        
        logger.info(f"Identified {len(hub_candidates)} hub candidates")
        
        # Print summary by region
        print("\n📊 HUB CANDIDATES BY REGION:")
        region_summary = hub_candidates.groupby('continent_name').agg({
            'iata_code': 'count',
            'hub_priority_score': 'mean',
            'runway_capacity': lambda x: (x == 'large').sum()
        }).round(2)
        region_summary.columns = ['Count', 'Avg Priority Score', 'Large Runways']
        print(region_summary)
        
        return hub_candidates
    
    def save_processed_data(self, df_all: pd.DataFrame, df_candidates: pd.DataFrame):
        """Save processed data to files"""
        logger.info("Saving processed airport data...")
        
        # Save complete processed dataset
        all_airports_path = os.path.join(self.output_dir, 'airports_processed.csv')
        df_all.to_csv(all_airports_path, index=False)
        logger.info(f"Saved complete processed dataset: {all_airports_path}")
        
        # Save hub candidates dataset
        candidates_path = os.path.join(self.output_dir, 'hub_candidates.csv')
        df_candidates.to_csv(candidates_path, index=False)
        logger.info(f"Saved hub candidates dataset: {candidates_path}")
        
        # Save summary statistics
        summary_path = os.path.join(self.output_dir, 'airport_processing_summary.txt')
        with open(summary_path, 'w') as f:
            f.write("AIRPORT DATA PROCESSING SUMMARY\n")
            f.write("="*50 + "\n\n")
            f.write(f"Total airports processed: {len(df_all)}\n")
            f.write(f"Hub candidates identified: {len(df_candidates)}\n")
            f.write(f"Countries represented: {df_all['iso_country'].nunique()}\n")
            f.write(f"Continents represented: {df_all['continent_name'].nunique()}\n\n")
            
            f.write("TOP 10 HUB CANDIDATES:\n")
            for i, (_, row) in enumerate(df_candidates.head(10).iterrows(), 1):
                f.write(f"{i:2d}. {row['name']} ({row['iata_code']}) - Score: {row['hub_priority_score']}\n")
        
        logger.info(f"Saved processing summary: {summary_path}")
        
        return all_airports_path, candidates_path
    
    def process_airport_data(self):
        """Main processing pipeline"""
        logger.info("Starting airport data processing pipeline...")
        
        # Load raw data
        df_raw = self.load_raw_data()
        
        # Clean and filter
        df_cleaned = self.clean_and_filter_data(df_raw)
        
        # Enrich with hub analysis fields
        df_enriched = self.enrich_airport_data(df_cleaned)
        
        # Create hub candidates dataset
        df_candidates = self.create_hub_candidates_dataset(df_enriched)
        
        # Save processed data
        all_path, candidates_path = self.save_processed_data(df_enriched, df_candidates)
        
        logger.info("Airport data processing completed successfully!")
        
        return {
            'all_airports': df_enriched,
            'hub_candidates': df_candidates,
            'all_airports_file': all_path,
            'candidates_file': candidates_path
        }

def main():
    """Main function"""
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python airport_csv_processor.py <csv_file_path>")
        print("Example: python airport_csv_processor.py Airports28062017_189278238873247918.csv")
        return 1
    
    csv_file = sys.argv[1]
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return 1
    
    try:
        # Process airport data
        processor = AirportCSVProcessor(csv_file)
        results = processor.process_airport_data()
        
        print(f"\n✅ Processing completed successfully!")
        print(f"📁 Files saved in: data/ArcGIS_Hub/")
        print(f"📊 Total airports: {len(results['all_airports'])}")
        print(f"🎯 Hub candidates: {len(results['hub_candidates'])}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error processing airport data: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())