"""
KLM Hub Summary Statistics - Individual Script

Creates an infographic-style summary with key statistics and findings
for the poster. Shows the most important numbers at a glance.
"""

import pandas as pd
import matplotlib.pyplot as plt
from rdflib import Graph
import os
import sys

class HubSummaryStats:
    def __init__(self, rdf_file_path):
        """Initialize with RDF file"""
        self.graph = Graph()
        self.graph.parse(rdf_file_path)
        
        # KLM Brand Colors
        self.klm_blue = '#00A1C9'
        self.klm_orange = '#FF6600'
        self.klm_dark_blue = '#003366'
        self.klm_gray = '#666666'
        self.klm_light_blue = '#87CEEB'
        
        # Create output directory
        self.output_dir = 'poster_visualizations'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def query_summary_data(self):
        """Query knowledge graph for summary statistics"""
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
            FILTER(?routeCount >= 3)
            
            OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
            OPTIONAL { ?airport klm:delayRate ?delayRate }
            
            OPTIONAL { 
                ?airport klm:locatedIn ?city .
                ?city klm:locatedIn ?countryUri .
                ?countryUri klm:name ?country .
            }
        }
        ORDER BY DESC(?hubScore)
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
    
    def create_summary_infographic(self):
        """Create infographic-style summary for poster"""
        print("🔍 Querying knowledge graph for summary data...")
        df = self.query_summary_data()
        
        if len(df) == 0:
            print("❌ No data found for summary")
            return None
        
        print(f"📊 Creating summary from {len(df)} airports")
        
        # Create infographic figure
        fig = plt.figure(figsize=(16, 10))
        fig.patch.set_facecolor('white')
        
        # Main title
        fig.text(0.5, 0.95, 'KLM HUB EXPANSION: KEY FINDINGS', 
                ha='center', va='top', fontsize=28, fontweight='bold', 
                color=self.klm_dark_blue)
        
        # Subtitle
        fig.text(0.5, 0.90, 'Data-Driven Analysis of European Airport Hub Potential', 
                ha='center', va='top', fontsize=16, style='italic', 
                color=self.klm_gray)
        
        # Calculate key statistics
        valid_passenger_data = df[df['Passenger Volume (M)'] > 0]
        
        # Key statistics boxes
        stats = [
            {
                'value': f"{len(df)}",
                'label': "Airports\nAnalyzed",
                'color': self.klm_blue
            },
            {
                'value': f"{df.iloc[0]['Airport Code']}" if len(df) > 0 else "N/A",
                'label': "Top Hub\nCandidate",
                'color': self.klm_orange
            },
            {
                'value': f"{df.iloc[0]['Hub Score']:.1f}" if len(df) > 0 else "0",
                'label': "Highest Hub\nScore",
                'color': self.klm_blue
            },
            {
                'value': f"{df['Route Count'].max()}" if len(df) > 0 else "0",
                'label': "Max Routes\nper Airport",
                'color': self.klm_orange
            },
            {
                'value': f"{valid_passenger_data['Passenger Volume (M)'].max():.0f}M" if len(valid_passenger_data) > 0 else "N/A",
                'label': "Largest Airport\n(Passengers)",
                'color': self.klm_blue
            }
        ]
        
        # Draw statistics boxes
        box_width = 0.15
        start_x = 0.1
        
        for i, stat in enumerate(stats):
            x = start_x + i * (box_width + 0.05)
            
            # Draw box
            rect = plt.Rectangle((x, 0.65), box_width, 0.15, 
                               facecolor=stat['color'], alpha=0.2, 
                               edgecolor=stat['color'], linewidth=2)
            fig.add_artist(rect)
            
            # Add value
            fig.text(x + box_width/2, 0.75, stat['value'], 
                    ha='center', va='center', fontsize=24, fontweight='bold',
                    color=stat['color'])
            
            # Add label
            fig.text(x + box_width/2, 0.68, stat['label'], 
                    ha='center', va='center', fontsize=12, fontweight='bold',
                    color=self.klm_dark_blue)
        
        # Top 3 recommendations
        fig.text(0.05, 0.55, 'TOP 3 HUB CANDIDATES:', 
                fontsize=18, fontweight='bold', color=self.klm_dark_blue)
        
        top_3 = df.head(3)
        for i, (_, row) in enumerate(top_3.iterrows()):
            y_pos = 0.48 - i * 0.08
            
            # Rank circle
            circle = plt.Circle((0.08, y_pos), 0.02, color=self.klm_blue, alpha=0.8)
            fig.add_artist(circle)
            fig.text(0.08, y_pos, str(i+1), ha='center', va='center', 
                    fontsize=16, fontweight='bold', color='white')
            
            # Airport info
            fig.text(0.12, y_pos + 0.01, f"{row['Airport']} ({row['Airport Code']})", 
                    fontsize=14, fontweight='bold', color=self.klm_dark_blue)
            fig.text(0.12, y_pos - 0.02, 
                    f"Score: {row['Hub Score']:.1f} | Routes: {row['Route Count']} | "
                    f"Passengers: {row['Passenger Volume (M)']:.1f}M",
                    fontsize=11, color=self.klm_gray)
        
        # Key insights
        fig.text(0.55, 0.55, 'KEY INSIGHTS:', 
                fontsize=18, fontweight='bold', color=self.klm_dark_blue)
        
        # Calculate insights
        avg_delay = df['Delay Rate'].mean() if len(df) > 0 else 0
        low_delay_count = len(df[df['Delay Rate'] < 0.15]) if len(df) > 0 else 0
        countries = df['Country'].nunique() if len(df) > 0 else 0
        avg_routes = df['Route Count'].mean() if len(df) > 0 else 0
        
        insights = [
            f"• Hub potential strongly correlates with route connectivity",
            f"• {low_delay_count} airports have excellent on-time performance (<15% delays)",
            f"• {countries} European countries represented in analysis",
            f"• Average routes per candidate airport: {avg_routes:.1f}",
            f"• Average delay rate across candidates: {avg_delay:.1%}"
        ]
        
        for i, insight in enumerate(insights):
            fig.text(0.55, 0.48 - i * 0.05, insight, 
                    fontsize=12, color=self.klm_dark_blue)
        
        # Methodology box
        method_text = """METHODOLOGY:
        • Knowledge Graph integration of KLM & Schiphol APIs
        • Multi-criteria analysis: connectivity, passengers, delays
        • Network centrality and hub potential scoring
        • Geographic and operational constraint evaluation"""
        
        fig.text(0.55, 0.25, method_text, 
                fontsize=11, color=self.klm_dark_blue,
                bbox=dict(boxstyle="round,pad=0.5", facecolor=self.klm_light_blue, alpha=0.3))
        
        # Data sources
        fig.text(0.05, 0.15, 'DATA SOURCES: KLM API, Schiphol API, Eurostat', 
                fontsize=10, style='italic', color=self.klm_gray)
        
        # Footer
        fig.text(0.5, 0.05, '🔍 Knowledge Engineering Project | TU Eindhoven | 2AMD20', 
                ha='center', fontsize=12, color=self.klm_gray)
        
        # Remove axes
        fig.add_subplot(111, frameon=False)
        plt.tick_params(labelcolor="none", bottom=False, left=False)
        
        # Save infographic
        output_path = os.path.join(self.output_dir, 'hub_summary_infographic.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        
        print(f"✅ Summary infographic saved: {output_path}")
        
        # Show plot
        plt.show()
        
        # Print detailed summary
        print(f"\n📋 DETAILED SUMMARY:")
        print(f"Total airports analyzed: {len(df)}")
        if len(df) > 0:
            print(f"Top candidate: {df.iloc[0]['Airport']} ({df.iloc[0]['Airport Code']})")
            print(f"Highest hub score: {df.iloc[0]['Hub Score']:.1f}")
            print(f"Score range: {df['Hub Score'].min():.1f} - {df['Hub Score'].max():.1f}")
            print(f"Average routes per airport: {df['Route Count'].mean():.1f}")
            print(f"Countries represented: {df['Country'].nunique()}")
        
        return fig, df

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python hub_summary_stats.py <rdf_file_path>")
        print("Example: python hub_summary_stats.py data/knowledge_graph/unified_kg.ttl")
        return 1
    
    rdf_file = sys.argv[1]
    
    if not os.path.exists(rdf_file):
        print(f"❌ RDF file not found: {rdf_file}")
        return 1
    
    try:
        print("📊 Creating KLM Hub Summary Statistics...")
        stats_maker = HubSummaryStats(rdf_file)
        fig, data = stats_maker.create_summary_infographic()
        
        if fig is not None:
            print("🎉 Summary infographic created successfully!")
            print("📁 Check the 'poster_visualizations' folder for the output.")
        else:
            print("❌ Failed to create summary - no data found")
            return 1
            
    except Exception as e:
        print(f"❌ Error creating summary: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())