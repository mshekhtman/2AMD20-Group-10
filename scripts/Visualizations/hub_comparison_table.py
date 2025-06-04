"""
KLM Hub Comparison Table - Individual Script

Creates a clean comparison table showing the top hub candidates
with their key metrics in an easy-to-read format for the poster.
"""

import pandas as pd
import matplotlib.pyplot as plt
from rdflib import Graph
import os
import sys

class HubComparisonTable:
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
    
    def query_comparison_data(self):
        """Query knowledge graph for comparison table data"""
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
        LIMIT 10
        """
        
        results = self.graph.query(query)
        data = []
        
        for row in results:
            airport_code = str(row.airport).split('/')[-1] if row.airport else 'Unknown'
            data.append({
                'Rank': len(data) + 1,
                'Airport': str(row.name) if row.name else airport_code,
                'Code': airport_code,
                'Country': str(row.country) if row.country else 'Unknown',
                'Hub Score': float(row.hubScore) if row.hubScore else 0,
                'Routes': int(row.routeCount) if row.routeCount else 0,
                'Passengers (M)': round(int(row.passengerVolume)/1000000, 1) if row.passengerVolume else 0,
                'Delay Rate': f"{float(row.delayRate)*100:.1f}%" if row.delayRate else "N/A"
            })
        
        return pd.DataFrame(data)
    
    def create_comparison_table(self):
        """Create professional comparison table"""
        print("🔍 Querying knowledge graph for comparison data...")
        df = self.query_comparison_data()
        
        if len(df) == 0:
            print("❌ No data found for comparison table")
            return None
        
        print(f"📊 Creating comparison table with {len(df)} candidates")
        
        # Create figure for table
        fig, ax = plt.subplots(figsize=(16, 10))
        ax.axis('tight')
        ax.axis('off')
        
        # Create table data with better formatting
        table_data = []
        headers = ['Rank', 'Airport', 'Code', 'Country', 'Hub Score', 'Routes', 'Passengers (M)', 'Delay Rate']
        
        for _, row in df.iterrows():
            table_data.append([
                f"#{row['Rank']}",
                row['Airport'][:25] + '...' if len(row['Airport']) > 25 else row['Airport'],
                row['Code'],
                row['Country'],
                f"{row['Hub Score']:.1f}",
                str(row['Routes']),
                f"{row['Passengers (M)']}M" if row['Passengers (M)'] > 0 else "N/A",
                row['Delay Rate']
            ])
        
        # Create table
        table = ax.table(cellText=table_data,
                        colLabels=headers,
                        cellLoc='center',
                        loc='center',
                        colWidths=[0.08, 0.25, 0.08, 0.15, 0.12, 0.08, 0.12, 0.12])
        
        # Style the table
        table.auto_set_font_size(False)
        table.set_fontsize(12)
        table.scale(1, 2)
        
        # Color header row
        for i in range(len(headers)):
            table[(0, i)].set_facecolor(self.klm_blue)
            table[(0, i)].set_text_props(weight='bold', color='white')
            table[(0, i)].set_height(0.08)
        
        # Color and style data rows
        for i in range(1, len(table_data) + 1):
            for j in range(len(headers)):
                if i <= 3:  # Top 3 candidates
                    table[(i, j)].set_facecolor(self.klm_light_blue)
                    table[(i, j)].set_alpha(0.3)
                elif i <= 6:  # Next 3 candidates
                    table[(i, j)].set_facecolor(self.klm_orange)
                    table[(i, j)].set_alpha(0.2)
                else:  # Remaining candidates
                    table[(i, j)].set_facecolor('lightgray')
                    table[(i, j)].set_alpha(0.1)
                
                # Bold text for top 3
                if i <= 3:
                    table[(i, j)].set_text_props(weight='bold')
                
                table[(i, j)].set_height(0.06)
        
        # Add title
        plt.title('KLM HUB EXPANSION CANDIDATES: COMPREHENSIVE COMPARISON', 
                 fontsize=20, fontweight='bold', pad=30, color=self.klm_dark_blue)
        
        # Add subtitle
        plt.figtext(0.5, 0.92, 'Multi-Criteria Evaluation of European Airport Hub Potential', 
                   ha='center', fontsize=14, style='italic', color=self.klm_gray)
        
        # Add legend
        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, facecolor=self.klm_light_blue, alpha=0.3, label='Top 3 Candidates'),
            plt.Rectangle((0, 0), 1, 1, facecolor=self.klm_orange, alpha=0.2, label='Strong Candidates'),
            plt.Rectangle((0, 0), 1, 1, facecolor='lightgray', alpha=0.1, label='Other Candidates')
        ]
        
        ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1, 0.15))
        
        # Add methodology note
        method_note = ("Hub Score calculated from: Route Connectivity + Passenger Volume + "
                      "Delay Performance + Geographic Strategic Value")
        plt.figtext(0.5, 0.08, method_note, ha='center', fontsize=10, 
                   style='italic', color=self.klm_gray)
        
        # Add data source
        plt.figtext(0.5, 0.05, 'Data Sources: KLM API, Schiphol API, Eurostat | Knowledge Engineering 2AMD20', 
                   ha='center', fontsize=9, color=self.klm_gray)
        
        # Save table
        output_path = os.path.join(self.output_dir, 'hub_comparison_table.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        
        print(f"✅ Comparison table saved: {output_path}")
        
        # Show plot
        plt.show()
        
        # Save CSV version for reference
        csv_path = os.path.join(self.output_dir, 'hub_comparison_data.csv')
        df.to_csv(csv_path, index=False)
        print(f"📊 Data also saved as CSV: {csv_path}")
        
        # Print summary
        print(f"\n📋 TOP 5 HUB CANDIDATES:")
        for _, row in df.head(5).iterrows():
            print(f"{row['Rank']}. {row['Airport']} ({row['Code']}) - Score: {row['Hub Score']:.1f}")
        
        return fig, df

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python hub_comparison_table.py <rdf_file_path>")
        print("Example: python hub_comparison_table.py data/knowledge_graph/unified_kg.ttl")
        return 1
    
    rdf_file = sys.argv[1]
    
    if not os.path.exists(rdf_file):
        print(f"❌ RDF file not found: {rdf_file}")
        return 1
    
    try:
        print("📊 Creating KLM Hub Comparison Table...")
        table_maker = HubComparisonTable(rdf_file)
        fig, data = table_maker.create_comparison_table()
        
        if fig is not None:
            print("🎉 Comparison table created successfully!")
            print("📁 Check the 'poster_visualizations' folder for the output.")
        else:
            print("❌ Failed to create table - no data found")
            return 1
            
    except Exception as e:
        print(f"❌ Error creating comparison table: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())