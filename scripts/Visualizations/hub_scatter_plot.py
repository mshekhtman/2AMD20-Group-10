"""
KLM Hub Scatter Plot Analysis - Individual Script

Creates scatter plot showing relationship between hub score, passenger volume, 
and other key metrics for hub candidate evaluation.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from rdflib import Graph
import numpy as np
import os
import sys

class HubScatterPlot:
    def __init__(self, rdf_file_path):
        """Initialize with RDF file"""
        self.graph = Graph()
        self.graph.parse(rdf_file_path)
        
        # KLM Brand Colors
        self.klm_blue = '#00A1C9'
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
            'font.size': 12,
            'axes.titlesize': 16,
            'axes.labelsize': 14,
            'xtick.labelsize': 12,
            'ytick.labelsize': 12,
            'legend.fontsize': 12,
            'figure.titlesize': 18
        })
    
    def query_hub_data(self):
        """Query knowledge graph for hub analysis data"""
        query = """
        PREFIX klm: <http://example.org/klm/>
        PREFIX sch: <http://example.org/schiphol/>
        
        SELECT ?airport ?name ?country ?hubScore ?routeCount ?passengerVolume 
               ?delayRate ?connectivityIndex
        WHERE {
            ?airport a klm:Airport ;
                    klm:name ?name ;
                    klm:hubPotentialScore ?hubScore ;
                    klm:routeCount ?routeCount .
            
            FILTER NOT EXISTS { ?airport klm:isMainHub true }
            FILTER(?routeCount >= 3)
            
            OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
            OPTIONAL { ?airport klm:delayRate ?delayRate }
            OPTIONAL { ?airport klm:connectivityIndex ?connectivityIndex }
            
            OPTIONAL { 
                ?airport klm:locatedIn ?city .
                ?city klm:locatedIn ?countryUri .
                ?countryUri klm:name ?country .
            }
        }
        ORDER BY DESC(?hubScore)
        LIMIT 20
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
                'Delay Rate': float(row.delayRate) if row.delayRate else 0,
                'Connectivity': float(row.connectivityIndex) if row.connectivityIndex else 0
            })
        
        return pd.DataFrame(data)
    
    def create_scatter_analysis(self):
        """Create comprehensive scatter plot analysis"""
        print("🔍 Querying knowledge graph for scatter plot data...")
        df = self.query_hub_data()
        
        if len(df) == 0:
            print("❌ No data found for scatter plot")
            return None
        
        print(f"📊 Analyzing {len(df)} airports")
        
        # Filter data with passenger volume > 0 for meaningful analysis
        valid_data = df[df['Passenger Volume (M)'] > 0]
        
        if len(valid_data) == 0:
            print("❌ No airports with passenger volume data")
            return None
        
        # Create figure with subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Hub Score vs Passenger Volume (main analysis)
        scatter1 = ax1.scatter(valid_data['Passenger Volume (M)'], valid_data['Hub Score'],
                              s=valid_data['Route Count']*15, 
                              c=valid_data['Delay Rate'], cmap='RdYlBu_r',
                              alpha=0.8, edgecolors=self.klm_dark_blue, linewidth=2)
        
        # Add airport labels for top candidates
        top_5 = valid_data.nlargest(5, 'Hub Score')
        for _, row in top_5.iterrows():
            ax1.annotate(row['Airport Code'], 
                        (row['Passenger Volume (M)'], row['Hub Score']),
                        xytext=(8, 8), textcoords='offset points', 
                        fontsize=10, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        
        ax1.set_xlabel('Annual Passenger Volume (Millions)', fontweight='bold')
        ax1.set_ylabel('Hub Potential Score', fontweight='bold')
        ax1.set_title('Hub Score vs Market Size\n(Bubble size = Route Count, Color = Delay Rate)', 
                     fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar1 = plt.colorbar(scatter1, ax=ax1)
        cbar1.set_label('Delay Rate', fontweight='bold')
        
        # Add trend line
        if len(valid_data) > 1:
            z = np.polyfit(valid_data['Passenger Volume (M)'], valid_data['Hub Score'], 1)
            p = np.poly1d(z)
            ax1.plot(valid_data['Passenger Volume (M)'], p(valid_data['Passenger Volume (M)']), 
                    "r--", alpha=0.8, linewidth=2, label=f'Trend (R² = {np.corrcoef(valid_data["Passenger Volume (M)"], valid_data["Hub Score"])[0,1]**2:.2f})')
            ax1.legend()
        
        # 2. Route Count vs Hub Score
        scatter2 = ax2.scatter(valid_data['Route Count'], valid_data['Hub Score'],
                              s=80, c=self.klm_blue, alpha=0.7, edgecolors=self.klm_dark_blue)
        
        ax2.set_xlabel('Number of Routes', fontweight='bold')
        ax2.set_ylabel('Hub Potential Score', fontweight='bold')
        ax2.set_title('Connectivity vs Hub Potential', fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # Add trend line
        if len(valid_data) > 1:
            z2 = np.polyfit(valid_data['Route Count'], valid_data['Hub Score'], 1)
            p2 = np.poly1d(z2)
            ax2.plot(valid_data['Route Count'], p2(valid_data['Route Count']), 
                    "r--", alpha=0.8, linewidth=2)
        
        # 3. Delay Rate vs Passenger Volume
        colors_delay = ['green' if x < 0.15 else 'orange' if x < 0.25 else 'red' 
                       for x in valid_data['Delay Rate']]
        
        ax3.scatter(valid_data['Passenger Volume (M)'], valid_data['Delay Rate']*100,
                   s=80, c=colors_delay, alpha=0.7, edgecolors=self.klm_dark_blue)
        
        ax3.set_xlabel('Annual Passenger Volume (Millions)', fontweight='bold')
        ax3.set_ylabel('Delay Rate (%)', fontweight='bold')
        ax3.set_title('Market Size vs Operational Performance', fontweight='bold')
        ax3.grid(True, alpha=0.3)
        
        # 4. Multi-dimensional bubble chart
        scatter4 = ax4.scatter(valid_data['Route Count'], valid_data['Passenger Volume (M)'],
                              s=valid_data['Hub Score']*20, 
                              c=valid_data['Delay Rate'], cmap='RdYlGn_r',
                              alpha=0.7, edgecolors=self.klm_dark_blue, linewidth=2)
        
        ax4.set_xlabel('Number of Routes', fontweight='bold')
        ax4.set_ylabel('Passenger Volume (Millions)', fontweight='bold')
        ax4.set_title('Routes vs Passengers\n(Bubble size = Hub Score, Color = Delay Rate)', 
                     fontweight='bold')
        ax4.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar4 = plt.colorbar(scatter4, ax=ax4)
        cbar4.set_label('Delay Rate', fontweight='bold')
        
        # Overall title
        fig.suptitle('KLM HUB CANDIDATE ANALYSIS: KEY PERFORMANCE RELATIONSHIPS', 
                    fontsize=20, fontweight='bold', y=0.98, color=self.klm_dark_blue)
        
        # Adjust layout
        plt.tight_layout()
        plt.subplots_adjust(top=0.92)
        
        # Save plot
        output_path = os.path.join(self.output_dir, 'hub_scatter_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        
        print(f"✅ Scatter analysis saved: {output_path}")
        
        # Show plot
        plt.show()
        
        # Print correlation analysis
        print(f"\n📊 CORRELATION ANALYSIS:")
        correlations = valid_data[['Hub Score', 'Route Count', 'Passenger Volume (M)', 'Delay Rate']].corr()
        print(f"Hub Score vs Passenger Volume: {correlations.loc['Hub Score', 'Passenger Volume (M)']:.3f}")
        print(f"Hub Score vs Route Count: {correlations.loc['Hub Score', 'Route Count']:.3f}")
        print(f"Hub Score vs Delay Rate: {correlations.loc['Hub Score', 'Delay Rate']:.3f}")
        
        return fig, valid_data

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python hub_scatter_plot.py <rdf_file_path>")
        print("Example: python hub_scatter_plot.py data/knowledge_graph/unified_kg.ttl")
        return 1
    
    rdf_file = sys.argv[1]
    
    if not os.path.exists(rdf_file):
        print(f"❌ RDF file not found: {rdf_file}")
        return 1
    
    try:
        print("📊 Creating KLM Hub Scatter Plot Analysis...")
        plot_maker = HubScatterPlot(rdf_file)
        fig, data = plot_maker.create_scatter_analysis()
        
        if fig is not None:
            print("🎉 Scatter plot analysis created successfully!")
            print("📁 Check the 'poster_visualizations' folder for the output.")
        else:
            print("❌ Failed to create plot - no data found")
            return 1
            
    except Exception as e:
        print(f"❌ Error creating scatter plot: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())