"""
KLM Hub Ranking Chart - Individual Script

Creates the main hub ranking visualization for the poster.
This is the centerpiece chart showing top hub candidates.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from rdflib import Graph
import os
import sys

class HubRankingChart:
    def __init__(self, rdf_file_path):
        """Initialize with RDF file"""
        self.graph = Graph()
        self.graph.parse(rdf_file_path)
        
        # KLM Brand Colors
        self.klm_blue = '#00A1C9'
        self.klm_light_blue = '#87CEEB'
        self.klm_orange = '#FF6600'
        self.klm_dark_blue = '#003366'
        self.klm_gray = '#666666'
        
        self.setup_style()
        
        # Create output directory
        self.output_dir = 'poster_visualizations'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def setup_style(self):
        """Configure matplotlib for poster quality"""
        plt.rcParams.update({
            'font.size': 14,
            'axes.titlesize': 18,
            'axes.labelsize': 16,
            'xtick.labelsize': 14,
            'ytick.labelsize': 14,
            'legend.fontsize': 14,
            'figure.titlesize': 20,
            'font.family': 'sans-serif'
        })
    
    def query_hub_candidates(self):
        """Query knowledge graph for hub candidates"""
        query = """
        PREFIX klm: <http://example.org/klm/>
        PREFIX sch: <http://example.org/schiphol/>
        
        SELECT ?airport ?name ?country ?hubScore ?routeCount ?passengerVolume ?delayRate
        WHERE {
            ?airport a klm:Airport ;
                    klm:name ?name ;
                    klm:hubPotentialScore ?hubScore ;
                    klm:routeCount ?routeCount .
            
            FILTER NOT EXISTS { ?airport klm:isMainHub true }
            FILTER(?routeCount >= 5)
            
            OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
            OPTIONAL { ?airport klm:delayRate ?delayRate }
            
            OPTIONAL { 
                ?airport klm:locatedIn ?city .
                ?city klm:locatedIn ?countryUri .
                ?countryUri klm:name ?country .
            }
        }
        ORDER BY DESC(?hubScore)
        LIMIT 15
        """
        
        results = self.graph.query(query)
        data = []
        
        for row in results:
            airport_code = str(row.airport).split('/')[-1] if row.airport else 'Unknown'
            data.append({
                'Airport Code': airport_code,
                'Airport': str(row.name) if row.name else airport_code,
                'Country': str(row.country) if row.country else 'Unknown',
                'Hub Score': float(row.hubScore) if row.hubScore else 0,
                'Route Count': int(row.routeCount) if row.routeCount else 0,
                'Passenger Volume (M)': int(row.passengerVolume)/1000000 if row.passengerVolume else 0,
                'Delay Rate': float(row.delayRate) if row.delayRate else 0
            })
        
        return pd.DataFrame(data)
    
    def create_hub_ranking_chart(self):
        """Create the main hub ranking chart"""
        print("🔍 Querying knowledge graph for hub candidates...")
        df = self.query_hub_candidates()
        
        if len(df) == 0:
            print("❌ No hub candidates found in knowledge graph")
            return None
        
        print(f"📊 Found {len(df)} hub candidates")
        
        # Take top 8 for clean visualization
        top_8 = df.head(8)
        
        # Create figure
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Create horizontal bar chart
        bars = ax.barh(range(len(top_8)), top_8['Hub Score'], 
                      color=[self.klm_blue if i < 3 else self.klm_light_blue for i in range(len(top_8))],
                      edgecolor=self.klm_dark_blue, linewidth=2)
        
        # Customize the chart
        ax.set_yticks(range(len(top_8)))
        ax.set_yticklabels([f"{row['Airport']}\n({row['Country']})" for _, row in top_8.iterrows()], 
                          fontsize=16, fontweight='bold')
        ax.set_xlabel('Hub Potential Score', fontsize=18, fontweight='bold')
        ax.set_title('🎯 TOP CANDIDATES FOR KLM\'S SECOND HUB', 
                    fontsize=22, fontweight='bold', pad=20, color=self.klm_dark_blue)
        
        # Add value labels on bars
        for i, (bar, (_, row)) in enumerate(zip(bars, top_8.iterrows())):
            width = bar.get_width()
            # Add score
            ax.text(width + 0.5, bar.get_y() + bar.get_height()/2, 
                   f'{width:.1f}', ha='left', va='center', 
                   fontweight='bold', fontsize=16)
            
            # Add route count as secondary info
            ax.text(width/2, bar.get_y() + bar.get_height()/2, 
                   f'{row["Route Count"]} routes', ha='center', va='center', 
                   fontweight='bold', fontsize=12, color='white')
        
        # Invert y-axis to show highest score at top
        ax.invert_yaxis()
        
        # Add grid for better readability
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        ax.set_axisbelow(True)
        
        # Add KLM branding
        ax.text(0.98, 0.02, 'KLM Hub Expansion Analysis', 
               transform=ax.transAxes, ha='right', va='bottom',
               fontsize=12, style='italic', color=self.klm_gray)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save chart
        output_path = os.path.join(self.output_dir, 'hub_ranking_chart.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        
        print(f"✅ Hub ranking chart saved: {output_path}")
        
        # Show the plot
        plt.show()
        
        # Print summary
        print(f"\n📋 TOP 3 HUB CANDIDATES:")
        for i, (_, row) in enumerate(top_8.head(3).iterrows()):
            print(f"{i+1}. {row['Airport']} ({row['Airport Code']}) - Score: {row['Hub Score']:.1f}")
        
        return fig, top_8

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python hub_ranking_chart.py <rdf_file_path>")
        print("Example: python hub_ranking_chart.py data/knowledge_graph/unified_kg.ttl")
        return 1
    
    rdf_file = sys.argv[1]
    
    if not os.path.exists(rdf_file):
        print(f"❌ RDF file not found: {rdf_file}")
        return 1
    
    try:
        print("🎯 Creating KLM Hub Ranking Chart...")
        chart_maker = HubRankingChart(rdf_file)
        fig, data = chart_maker.create_hub_ranking_chart()
        
        if fig is not None:
            print("🎉 Hub ranking chart created successfully!")
            print("📁 Check the 'poster_visualizations' folder for the output.")
        else:
            print("❌ Failed to create chart - no data found")
            return 1
            
    except Exception as e:
        print(f"❌ Error creating hub ranking chart: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())