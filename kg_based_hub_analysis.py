"""
KLM Second Hub Analysis - Single Clean Visualization
===================================================

Creates one clear, poster-ready visualization showing hub candidates
from the knowledge graph data.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from rdflib import Graph
import seaborn as sns
import os
import glob
import warnings
warnings.filterwarnings('ignore')

def load_knowledge_graph():
    """Load the specific knowledge graph file"""
    kg_files = [
        'data/knowledge_graph/deduplicated_unified_klm_hub_kg_20250605_193757.ttl',
        'data/knowledge_graph/deduplicated_unified_klm_hub_kg_20250605_193757.rdf'
    ]
    
    kg_file = None
    for file_path in kg_files:
        if os.path.exists(file_path):
            kg_file = file_path
            break
    
    if not kg_file:
        patterns = [
            'data/knowledge_graph/deduplicated_unified_klm_hub_kg_*.ttl',
            'data/knowledge_graph/deduplicated_unified_klm_hub_kg_*.rdf'
        ]
        for pattern in patterns:
            files = glob.glob(pattern)
            if files:
                kg_file = max(files, key=os.path.getmtime)
                break
    
    if not kg_file:
        raise FileNotFoundError("Knowledge graph file not found")
    
    print(f"Loading: {kg_file}")
    g = Graph()
    g.parse(kg_file, format="turtle" if kg_file.endswith('.ttl') else "xml")
    print(f"Loaded {len(g)} triples")
    return g

def query_hub_candidates(g):
    """Query for hub candidates"""
    namespaces = """
    PREFIX klm: <http://example.org/klm/>
    PREFIX sch: <http://example.org/schiphol/>
    PREFIX arcgis: <http://example.org/arcgis/>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX schema: <http://schema.org/>
    """
    
    query = namespaces + """
    SELECT ?airport ?name ?code ?country 
           ?lat ?lng ?passengerVolume ?routeCount ?hubScore
    WHERE {
        ?airport a klm:Airport ;
                klm:code ?code .
        
        FILTER(?code != "AMS")
        
        OPTIONAL { ?airport klm:name ?name }
        OPTIONAL { ?airport schema:name ?name }
        OPTIONAL { ?airport geo:lat ?lat }
        OPTIONAL { ?airport geo:long ?lng }
        OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
        OPTIONAL { ?airport klm:routeCount ?routeCount }
        OPTIONAL { ?airport klm:hubPotentialScore ?hubScore }
        
        OPTIONAL { 
            ?airport klm:locatedIn ?city .
            ?city klm:locatedIn ?countryUri .
            ?countryUri klm:name ?country .
        }
        
        FILTER(BOUND(?passengerVolume) || BOUND(?routeCount) || BOUND(?hubScore))
    }
    ORDER BY DESC(?hubScore) DESC(?passengerVolume)
    """
    
    results = g.query(query)
    data = []
    
    for row in results:
        data.append({
            'name': str(row.name) if row.name else 'Unknown',
            'code': str(row.code) if row.code else 'Unknown',
            'country': str(row.country) if row.country else 'Unknown',
            'lat': float(row.lat) if row.lat else None,
            'lng': float(row.lng) if row.lng else None,
            'passenger_volume': int(row.passengerVolume) if row.passengerVolume else 0,
            'route_count': int(row.routeCount) if row.routeCount else 0,
            'hub_score': float(row.hubScore) if row.hubScore else 0,
        })
    
    return pd.DataFrame(data)

def enhance_data(df):
    """Enhance data with calculated metrics"""
    df = df.copy()
    df['passenger_volume_m'] = df['passenger_volume'] / 1000000
    
    # Calculate distance from Amsterdam if coordinates available
    ams_lat, ams_lng = 52.3105, 4.7683
    
    def calc_distance(row):
        if pd.notna(row['lat']) and pd.notna(row['lng']):
            lat1, lng1 = np.radians(ams_lat), np.radians(ams_lng)
            lat2, lng2 = np.radians(row['lat']), np.radians(row['lng'])
            dlat, dlng = lat2 - lat1, lng2 - lng1
            a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlng/2)**2
            return 6371 * 2 * np.arcsin(np.sqrt(a))  # Earth radius = 6371 km
        return None
    
    df['distance_km'] = df.apply(calc_distance, axis=1)
    
    # Calculate final hub score if needed
    if df['hub_score'].max() == 0:
        df['passenger_norm'] = df['passenger_volume_m'] / df['passenger_volume_m'].max() if df['passenger_volume_m'].max() > 0 else 0
        df['route_norm'] = df['route_count'] / df['route_count'].max() if df['route_count'].max() > 0 else 0
        df['final_hub_score'] = (df['passenger_norm'] * 0.6 + df['route_norm'] * 0.4) * 100
    else:
        df['final_hub_score'] = df['hub_score']
    
    return df

def create_hub_visualization(df):
    """Create comprehensive visualization showing all relevant hub candidates"""
    # Much more inclusive filtering to show comprehensive analysis
    df_filtered = df[
        (df['passenger_volume_m'] > 1) |  # Very low threshold - include small airports too
        (df['route_count'] > 1) |         # Any airport with routes
        (df['final_hub_score'] > 5)       # Lower hub score threshold
    ].copy()
    
    # Show many more candidates for thorough analysis
    df_sorted = df_filtered.sort_values('final_hub_score', ascending=False).head(50)  # Show top 50
    
    print(f"Showing {len(df_sorted)} airports out of {len(df_filtered)} candidates")
    
    # Set up the plot with larger figure for poster
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(20, 16))  # Even larger for more airports
    
    # Use a white background
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    # Determine x-axis data and calculate better limits
    if df_sorted['distance_km'].notna().any() and df_sorted['distance_km'].max() > 0:
        x_data = df_sorted['distance_km']
        x_label = 'Distance from Amsterdam (km)'
        
        # Calculate limits to show all data clearly
        x_min = max(0, df_sorted['distance_km'].min() * 0.92)
        x_max = df_sorted['distance_km'].max() * 1.08
        
        # Add some padding if data is clustered
        x_range = x_max - x_min
        if x_range < 1000:  # If data is too clustered, expand range
            x_center = (x_max + x_min) / 2
            x_min = max(0, x_center - 2500)
            x_max = x_center + 2500
    else:
        x_data = df_sorted['final_hub_score']
        x_label = 'Hub Potential Score'
        x_min = 0
        x_max = df_sorted['final_hub_score'].max() * 1.1
    
    # Better y-axis limits to show all data
    y_min = max(-5, df_sorted['passenger_volume_m'].min() * 0.9 - 5)  # Small negative for bottom spacing
    y_max = df_sorted['passenger_volume_m'].max() * 1.08
    
    # Create scatter plot with variable sizing based on data range
    max_score = df_sorted['final_hub_score'].max()
    min_size = 100  # Minimum bubble size
    max_size = 2000  # Maximum bubble size
    
    # Normalize bubble sizes
    normalized_scores = (df_sorted['final_hub_score'] - df_sorted['final_hub_score'].min()) / (max_score - df_sorted['final_hub_score'].min())
    bubble_sizes = min_size + (normalized_scores * (max_size - min_size))
    
    scatter = ax.scatter(
        x_data, 
        df_sorted['passenger_volume_m'],
        s=bubble_sizes,  # Variable sizing
        c=df_sorted['final_hub_score'], 
        cmap='RdYlBu_r',
        alpha=0.75,
        edgecolors='black',
        linewidth=2,
        zorder=5
    )
    
    # Add labels for ALL visible airports with smart positioning
    from adjustText import adjust_text
    texts = []
    
    for idx, row in df_sorted.iterrows():
        x_val = row['distance_km'] if pd.notna(row['distance_km']) else row['final_hub_score']
        
        # Create text annotation
        text = ax.annotate(
            row['code'], 
            (x_val, row['passenger_volume_m']),
            fontsize=11,
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.25', 
                     facecolor='white', 
                     alpha=0.9, 
                     edgecolor='black',
                     linewidth=1),
            ha='center',
            va='center',
            zorder=10,
            color='black'
        )
        texts.append(text)
    
    # Use adjust_text to prevent overlapping labels (if available)
    try:
        adjust_text(texts, 
                   only_move={'points': 'xy', 'texts': 'xy'},
                   arrowprops=dict(arrowstyle='->', color='gray', alpha=0.5, lw=0.5),
                   expand_points=(1.2, 1.2),
                   expand_text=(1.1, 1.1))
    except ImportError:
        # If adjust_text not available, use manual positioning
        print("Note: Install 'adjustText' package for better label positioning: pip install adjustText")
        
        # Manual positioning strategy for overlapping labels
        positions_used = []
        for i, (idx, row) in enumerate(df_sorted.iterrows()):
            x_val = row['distance_km'] if pd.notna(row['distance_km']) else row['final_hub_score']
            y_val = row['passenger_volume_m']
            
            # Find non-overlapping position
            offset_x, offset_y = 0, 15
            attempts = 0
            while attempts < 10:  # Try up to 10 positions
                new_pos = (x_val + offset_x, y_val + offset_y)
                too_close = False
                
                for used_pos in positions_used:
                    if abs(new_pos[0] - used_pos[0]) < 200 and abs(new_pos[1] - used_pos[1]) < 3:
                        too_close = True
                        break
                
                if not too_close:
                    positions_used.append(new_pos)
                    break
                
                # Try different offset
                offset_y += 8 if attempts % 2 == 0 else -8
                offset_x += 50 if attempts > 5 else 0
                attempts += 1
    
    # Large, bold styling for poster
    ax.set_xlabel(x_label, fontsize=24, fontweight='bold', labelpad=20)
    ax.set_ylabel('Annual Passengers (millions)', fontsize=24, fontweight='bold', labelpad=20)
    ax.set_title('KLM Second Hub Candidates - Comprehensive Analysis\nBubble Size = Hub Potential Score', 
                fontsize=28, fontweight='bold', pad=30)
    
    # Set explicit limits to show all data
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(y_min, y_max)
    
    # Bold grid for poster visibility
    ax.grid(True, alpha=0.4, linestyle='-', linewidth=1, zorder=1)
    
    # Large, bold ticks
    ax.tick_params(axis='both', which='major', labelsize=18, width=2, length=8)
    
    # Make axis spines more visible
    for spine in ax.spines.values():
        spine.set_linewidth(2)
        spine.set_color('black')
    
    # Large, prominent colorbar
    cbar = plt.colorbar(scatter, ax=ax, shrink=0.7, pad=0.02, aspect=25)
    cbar.set_label('Hub Potential Score', fontsize=22, fontweight='bold', labelpad=20)
    cbar.ax.tick_params(labelsize=18, width=2, length=6)
    
    # Make colorbar outline more visible
    cbar.outline.set_linewidth(2)
    
    # Add statistics text box
    stats_text = f"""Data Overview:
• {len(df_sorted)} airports analyzed
• Top score: {df_sorted['final_hub_score'].max():.1f}
• Distance range: {df_sorted['distance_km'].min():.0f}-{df_sorted['distance_km'].max():.0f} km
• Passenger range: {df_sorted['passenger_volume_m'].min():.0f}-{df_sorted['passenger_volume_m'].max():.0f}M"""
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
            fontsize=14, verticalalignment='top',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8),
            zorder=15)
    
    # Ensure tight layout with extra padding for poster
    plt.tight_layout(pad=3.0)
    
    # Save high-quality image optimized for poster printing
    output_file = 'KLM_Hub_Candidates_Analysis.png'
    plt.savefig(output_file, 
                dpi=300, 
                bbox_inches='tight', 
                facecolor='white', 
                edgecolor='none',
                format='png',
                pad_inches=0.5)
    
    print(f"Comprehensive visualization saved as: {output_file}")
    print(f"Showing top {len(df_sorted)} hub candidates with all airport codes labeled")
    
    # Close the plot to prevent hanging
    plt.close()
    
    return df_sorted

def main():
    """Main execution"""
    print("Loading Knowledge Graph...")
    g = load_knowledge_graph()
    
    print("Querying hub candidates...")
    df_raw = query_hub_candidates(g)
    
    print("Processing data...")
    df_enhanced = enhance_data(df_raw)
    
    print("Creating visualization...")
    df_results = create_hub_visualization(df_enhanced)
    
    print("\nTop 10 Hub Candidates:")
    print("=" * 50)
    for i, (_, row) in enumerate(df_results.head(10).iterrows(), 1):
        print(f"{i:2d}. {row['code']} - {row['name'][:30]}")
        print(f"     Score: {row['final_hub_score']:.1f} | "
              f"Passengers: {row['passenger_volume_m']:.0f}M | "
              f"Country: {row['country']}")
    
    print(f"\nVisualization saved as: KLM_Hub_Candidates_Analysis.png")
    return df_results

if __name__ == "__main__":
    results = main()