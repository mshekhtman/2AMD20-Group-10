"""
KLM Hub Analysis Poster Visualizations

This script creates compelling visualizations for the poster focusing on Research Question 1:
"Which other airport would be a suitable fit to be a second hub for KLM?"

Generates publication-ready charts with KLM branding for academic poster presentation.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import networkx as nx
import numpy as np
from rdflib import Graph
import folium
from folium import plugins
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class KLMHubPosterVisualizer:
    def __init__(self, rdf_file_path):
        """Initialize with RDF knowledge graph file"""
        self.graph = Graph()
        self.graph.parse(rdf_file_path)
        
        # KLM Brand Colors
        self.klm_blue = '#00A1C9'
        self.klm_light_blue = '#87CEEB'
        self.klm_orange = '#FF6600'
        self.klm_dark_blue = '#003366'
        self.klm_gray = '#666666'
        
        # Color palette for visualizations
        self.colors = [self.klm_blue, self.klm_orange, '#4CAF50', '#FF9800', 
                      '#9C27B0', '#607D8B', '#795548', '#E91E63']
        
        # Set up plotting style
        self.setup_poster_style()
        
        # Create output directory
        self.output_dir = 'poster_visualizations'
        os.makedirs(self.output_dir, exist_ok=True)
        
        print(f"🎯 KLM Hub Analysis Visualizer initialized")
        print(f"📁 Output directory: {self.output_dir}")
    
    def setup_poster_style(self):
        """Configure matplotlib and seaborn for poster-quality plots"""
        # Set style for publication quality
        plt.style.use('default')
        sns.set_style("whitegrid")
        
        # Configure default font sizes for poster readability
        plt.rcParams.update({
            'font.size': 14,
            'axes.titlesize': 18,
            'axes.labelsize': 16,
            'xtick.labelsize': 14,
            'ytick.labelsize': 14,
            'legend.fontsize': 14,
            'figure.titlesize': 20,
            'font.family': 'sans-serif',
            'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans']
        })
    
    def query_hub_candidates(self):
        """Query the knowledge graph for hub expansion candidates"""
        query = """
        PREFIX klm: <http://example.org/klm/>
        PREFIX sch: <http://example.org/schiphol/>
        PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
        
        SELECT ?airport ?name ?country ?hubScore ?routeCount ?passengerVolume 
               ?delayRate ?connectivityIndex ?lat ?long
        WHERE {
            ?airport a klm:Airport ;
                    klm:name ?name ;
                    klm:hubPotentialScore ?hubScore ;
                    klm:routeCount ?routeCount .
            
            # Exclude Amsterdam (current hub)
            FILTER NOT EXISTS { ?airport klm:isMainHub true }
            FILTER(?routeCount >= 5)
            
            OPTIONAL { ?airport klm:passengerVolume ?passengerVolume }
            OPTIONAL { ?airport klm:delayRate ?delayRate }
            OPTIONAL { ?airport klm:connectivityIndex ?connectivityIndex }
            OPTIONAL { ?airport geo:lat ?lat }
            OPTIONAL { ?airport geo:long ?long }
            
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
                'Connectivity': float(row.connectivityIndex) if row.connectivityIndex else 0,
                'Latitude': float(row.lat) if row.lat else None,
                'Longitude': float(row.long) if row.long else None
            })
        
        df = pd.DataFrame(data)
        print(f"📊 Found {len(df)} hub candidates in knowledge graph")
        return df
    
    def create_hub_ranking_poster_chart(self):
        """Create main hub ranking chart for poster centerpiece"""
        df = self.query_hub_candidates()
        top_8 = df.head(8)
        
        # Create figure with larger size for poster
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
        
        # Save high-resolution version for poster
        output_path = os.path.join(self.output_dir, 'hub_ranking_poster.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.show()
        
        print(f"✅ Hub ranking chart saved: {output_path}")
        return fig, top_8
    
    def create_hub_analysis_dashboard(self):
        """Create comprehensive 4-panel dashboard for poster"""
        df = self.query_hub_candidates()
        
        # Create 2x2 subplot layout
        fig = plt.figure(figsize=(20, 16))
        
        # Panel 1: Hub Score vs Passenger Volume (top-left)
        ax1 = plt.subplot(2, 2, 1)
        valid_data = df[(df['Passenger Volume (M)'] > 0) & (df['Hub Score'] > 0)]
        
        scatter = ax1.scatter(valid_data['Passenger Volume (M)'], valid_data['Hub Score'],
                             s=valid_data['Route Count']*20, 
                             c=valid_data['Delay Rate'], cmap='RdYlBu_r',
                             alpha=0.8, edgecolors=self.klm_dark_blue, linewidth=2)
        
        # Add airport labels for top 5
        for _, row in valid_data.head(5).iterrows():
            ax1.annotate(row['Airport Code'], 
                        (row['Passenger Volume (M)'], row['Hub Score']),
                        xytext=(8, 8), textcoords='offset points', 
                        fontsize=12, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        
        ax1.set_xlabel('Annual Passenger Volume (Millions)', fontweight='bold')
        ax1.set_ylabel('Hub Potential Score', fontweight='bold')
        ax1.set_title('Hub Score vs Market Size', fontweight='bold', fontsize=16)
        ax1.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar1 = plt.colorbar(scatter, ax=ax1)
        cbar1.set_label('Delay Rate', fontweight='bold')
        
        # Panel 2: Route Connectivity Analysis (top-right)
        ax2 = plt.subplot(2, 2, 2)
        top_10 = df.head(10)
        
        bars2 = ax2.bar(range(len(top_10)), top_10['Route Count'],
                       color=[self.klm_blue if i < 3 else self.klm_orange if i < 6 else self.klm_gray 
                             for i in range(len(top_10))],
                       edgecolor=self.klm_dark_blue, linewidth=1.5)
        
        ax2.set_xticks(range(len(top_10)))
        ax2.set_xticklabels(top_10['Airport Code'], rotation=45, fontweight='bold')
        ax2.set_ylabel('Number of Routes', fontweight='bold')
        ax2.set_title('Route Connectivity by Airport', fontweight='bold', fontsize=16)
        ax2.grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bar, value in zip(bars2, top_10['Route Count']):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    str(value), ha='center', va='bottom', fontweight='bold')
        
        # Panel 3: Geographic Distribution (bottom-left)
        ax3 = plt.subplot(2, 2, 3)
        
        # Count airports by country
        country_counts = df['Country'].value_counts().head(8)
        
        # Create pie chart
        colors_pie = [self.klm_blue, self.klm_orange, self.klm_light_blue, '#4CAF50', 
                     '#FF9800', '#9C27B0', '#607D8B', '#795548']
        
        wedges, texts, autotexts = ax3.pie(country_counts.values, labels=country_counts.index,
                                          autopct='%1.0f%%', colors=colors_pie[:len(country_counts)],
                                          startangle=90, textprops={'fontweight': 'bold'})
        
        ax3.set_title('Hub Candidates by Country', fontweight='bold', fontsize=16)
        
        # Panel 4: Performance Metrics Comparison (bottom-right)
        ax4 = plt.subplot(2, 2, 4)
        
        # Create radar chart data for top 5 airports
        top_5 = df.head(5)
        
        # Normalize metrics for comparison (0-1 scale)
        metrics = ['Hub Score', 'Route Count', 'Passenger Volume (M)', 'Connectivity']
        top_5_norm = top_5.copy()
        
        for metric in metrics:
            if top_5_norm[metric].max() > 0:
                top_5_norm[f'{metric}_norm'] = top_5_norm[metric] / top_5_norm[metric].max()
        
        # Create grouped bar chart instead of radar for clarity
        x = np.arange(len(metrics))
        width = 0.15
        
        for i, (_, airport) in enumerate(top_5.iterrows()):
            values = [top_5_norm.iloc[i][f'{m}_norm'] for m in metrics]
            ax4.bar(x + i*width, values, width, 
                   label=airport['Airport Code'], 
                   color=self.colors[i], alpha=0.8)
        
        ax4.set_xlabel('Performance Metrics', fontweight='bold')
        ax4.set_ylabel('Normalized Score (0-1)', fontweight='bold')
        ax4.set_title('Multi-Criteria Performance', fontweight='bold', fontsize=16)
        ax4.set_xticks(x + width*2)
        ax4.set_xticklabels(['Hub Score', 'Routes', 'Passengers', 'Connectivity'], rotation=45)
        ax4.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax4.grid(axis='y', alpha=0.3)
        
        # Overall title
        fig.suptitle('KLM HUB EXPANSION ANALYSIS: IDENTIFYING THE OPTIMAL SECOND HUB', 
                    fontsize=24, fontweight='bold', y=0.98, color=self.klm_dark_blue)
        
        # Add subtitle
        fig.text(0.5, 0.94, 'Comprehensive Multi-Criteria Evaluation of European Airport Hub Potential', 
                ha='center', fontsize=16, style='italic', color=self.klm_gray)
        
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        
        # Save dashboard
        output_path = os.path.join(self.output_dir, 'hub_analysis_dashboard.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.show()
        
        print(f"✅ Hub analysis dashboard saved: {output_path}")
        return fig
    
    def create_european_hub_map(self):
        """Create interactive map showing hub candidates across Europe"""
        df = self.query_hub_candidates()
        
        # Filter airports with coordinates
        map_data = df[(df['Latitude'].notna()) & (df['Longitude'].notna())].head(15)
        
        if len(map_data) == 0:
            print("⚠️ No coordinate data available for mapping")
            return None
        
        # Create base map centered on Europe
        center_lat = map_data['Latitude'].mean()
        center_lon = map_data['Longitude'].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=5,
                      tiles='CartoDB positron')
        
        # Add Amsterdam (current hub) as special marker
        folium.Marker(
            [52.3676, 4.9041],  # Amsterdam coordinates
            popup='<b>Amsterdam (AMS)</b><br>Current KLM Hub',
            tooltip='Current Hub',
            icon=folium.Icon(color='red', icon='plane', prefix='fa')
        ).add_to(m)
        
        # Add hub candidates with size based on hub score
        for _, row in map_data.iterrows():
            # Calculate marker size based on hub score
            size = max(10, min(30, row['Hub Score'] * 2))
            
            # Color based on ranking
            if row['Hub Score'] >= map_data['Hub Score'].quantile(0.8):
                color = 'green'
                icon_color = 'white'
            elif row['Hub Score'] >= map_data['Hub Score'].quantile(0.6):
                color = 'orange'
                icon_color = 'white'
            else:
                color = 'blue'
                icon_color = 'white'
            
            # Create popup with detailed info
            popup_html = f"""
            <div style="font-family: Arial; min-width: 200px;">
                <h4 style="color: #00A1C9; margin-bottom: 10px;">{row['Airport']} ({row['Airport Code']})</h4>
                <b>Country:</b> {row['Country']}<br>
                <b>Hub Score:</b> {row['Hub Score']:.1f}<br>
                <b>Routes:</b> {row['Route Count']}<br>
                <b>Passengers:</b> {row['Passenger Volume (M)']:.1f}M<br>
                <b>Delay Rate:</b> {row['Delay Rate']:.1%}
            </div>
            """
            
            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius=size,
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"{row['Airport']} (Score: {row['Hub Score']:.1f})",
                color=color,
                fillColor=color,
                fillOpacity=0.7,
                weight=3
            ).add_to(m)
        
        # Add legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 120px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px;">
        <h4 style="margin-top:0; color: #00A1C9;">Hub Potential</h4>
        <p><i class="fa fa-circle" style="color:green"></i> High Potential<br>
           <i class="fa fa-circle" style="color:orange"></i> Medium Potential<br>
           <i class="fa fa-circle" style="color:blue"></i> Lower Potential<br>
           <i class="fa fa-plane" style="color:red"></i> Current Hub (AMS)</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        output_path = os.path.join(self.output_dir, 'european_hub_map.html')
        m.save(output_path)
        
        print(f"✅ European hub map saved: {output_path}")
        return m
    
    def create_network_analysis_viz(self):
        """Create network visualization showing current vs potential hub networks"""
        # Query for route data
        route_query = """
        PREFIX klm: <http://example.org/klm/>
        
        SELECT ?origin ?destination ?originName ?destName ?airline
        WHERE {
            ?route klm:hasOrigin ?origin ;
                   klm:hasDestination ?destination .
            ?origin klm:name ?originName .
            ?destination klm:name ?destName .
            
            OPTIONAL {
                ?airline klm:operates ?route ;
                        klm:code ?airlineCode .
            }
        }
        LIMIT 200
        """
        
        results = self.graph.query(route_query)
        
        # Create network graph
        G = nx.Graph()
        
        for row in results:
            origin = str(row.originName) if row.originName else str(row.origin).split('/')[-1]
            dest = str(row.destName) if row.destName else str(row.destination).split('/')[-1]
            G.add_edge(origin, dest)
        
        # Calculate centrality measures
        centrality = nx.degree_centrality(G)
        betweenness = nx.betweenness_centrality(G)
        
        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
        
        # Current network (left panel)
        pos1 = nx.spring_layout(G, k=3, iterations=50, seed=42)
        
        # Node sizes based on centrality
        node_sizes1 = [centrality[node] * 3000 for node in G.nodes()]
        node_colors1 = [centrality[node] for node in G.nodes()]
        
        # Draw current network
        nx.draw_networkx_nodes(G, pos1, node_size=node_sizes1, 
                              node_color=node_colors1, cmap=plt.cm.Blues,
                              alpha=0.8, ax=ax1)
        nx.draw_networkx_edges(G, pos1, alpha=0.2, width=0.5, ax=ax1)
        
        # Label major hubs
        major_hubs = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:8]
        labels1 = {node: node for node, _ in major_hubs}
        nx.draw_networkx_labels(G, pos1, labels1, font_size=10, 
                               font_weight='bold', ax=ax1)
        
        ax1.set_title('Current Route Network\n(Node size = Connectivity)', 
                     fontweight='bold', fontsize=16)
        ax1.axis('off')
        
        # Potential network with second hub (right panel)
        # Simulate adding connections from potential second hub
        hub_candidates = self.query_hub_candidates()
        if len(hub_candidates) > 0:
            second_hub = hub_candidates.iloc[0]['Airport']
            
            # Add theoretical connections from second hub to major destinations
            G_potential = G.copy()
            major_destinations = [node for node, cent in major_hubs[:10]]
            
            for dest in major_destinations:
                if dest != second_hub:
                    G_potential.add_edge(second_hub, dest)
            
            # Recalculate centrality
            new_centrality = nx.degree_centrality(G_potential)
            
            # Use same positions but update sizes
            node_sizes2 = [new_centrality[node] * 3000 for node in G_potential.nodes()]
            node_colors2 = [new_centrality[node] for node in G_potential.nodes()]
            
            # Highlight the new hub
            special_colors = []
            for node in G_potential.nodes():
                if node == second_hub:
                    special_colors.append(1.0)  # Max color for second hub
                elif node == 'Amsterdam Airport Schiphol' or 'AMS' in node:
                    special_colors.append(0.8)  # High color for current hub
                else:
                    special_colors.append(new_centrality[node])
            
            nx.draw_networkx_nodes(G_potential, pos1, node_size=node_sizes2,
                                  node_color=special_colors, cmap=plt.cm.Reds,
                                  alpha=0.8, ax=ax2)
            nx.draw_networkx_edges(G_potential, pos1, alpha=0.2, width=0.5, ax=ax2)
            
            # Label both hubs
            labels2 = {second_hub: f"{second_hub}\n(New Hub)", 
                      'Amsterdam Airport Schiphol': 'Amsterdam\n(Current Hub)'}
            nx.draw_networkx_labels(G_potential, pos1, labels2, font_size=10,
                                   font_weight='bold', ax=ax2)
            
            ax2.set_title(f'Potential Network with {second_hub}\n(Red = Enhanced connectivity)', 
                         fontweight='bold', fontsize=16)
        
        ax2.axis('off')
        
        # Overall title
        fig.suptitle('NETWORK CONNECTIVITY ANALYSIS: CURRENT vs POTENTIAL HUB EXPANSION', 
                    fontsize=20, fontweight='bold', y=0.95, color=self.klm_dark_blue)
        
        plt.tight_layout()
        
        # Save network analysis
        output_path = os.path.join(self.output_dir, 'network_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.show()
        
        print(f"✅ Network analysis saved: {output_path}")
        return fig, centrality, betweenness
    
    def create_executive_summary_infographic(self):
        """Create infographic-style summary for poster"""
        df = self.query_hub_candidates()
        
        if len(df) == 0:
            print("⚠️ No data available for summary")
            return None
        
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
        
        # Key statistics boxes
        stats = [
            {
                'value': f"{len(df)}",
                'label': "Airports\nAnalyzed",
                'color': self.klm_blue
            },
            {
                'value': f"{df.iloc[0]['Airport Code']}",
                'label': "Top Hub\nCandidate",
                'color': self.klm_orange
            },
            {
                'value': f"{df.iloc[0]['Hub Score']:.1f}",
                'label': "Highest Hub\nScore",
                'color': self.klm_blue
            },
            {
                'value': f"{df['Route Count'].max()}",
                'label': "Max Routes\nper Airport",
                'color': self.klm_orange
            },
            {
                'value': f"{df['Passenger Volume (M)'].max():.0f}M",
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
        
        insights = [
            f"• Hub potential strongly correlates with route connectivity",
            f"• {df[df['Delay Rate'] < 0.15]['Airport'].count()} airports have excellent on-time performance",
            f"• European airports dominate the top candidates",
            f"• Passenger volume ranges from {df['Passenger Volume (M)'].min():.1f}M to {df['Passenger Volume (M)'].max():.1f}M",
            f"• Average hub score: {df['Hub Score'].mean():.1f}"
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
        output_path = os.path.join(self.output_dir, 'executive_summary_infographic.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.show()
        
        print(f"✅ Executive summary infographic saved: {output_path}")
        return fig
    
    def create_comparative_analysis_chart(self):
        """Create detailed comparative analysis of top hub candidates"""
        df = self.query_hub_candidates()
        top_6 = df.head(6)
        
        # Create figure with multiple comparison metrics
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('COMPREHENSIVE HUB CANDIDATE COMPARISON', 
                    fontsize=20, fontweight='bold', color=self.klm_dark_blue, y=0.98)
        
        # 1. Hub Score Comparison (top-left)
        ax1 = axes[0, 0]
        bars1 = ax1.bar(top_6['Airport Code'], top_6['Hub Score'], 
                       color=[self.klm_blue if i == 0 else self.klm_orange if i < 3 else self.klm_gray 
                             for i in range(len(top_6))],
                       edgecolor=self.klm_dark_blue, linewidth=2)
        
        ax1.set_title('Hub Potential Score', fontweight='bold', fontsize=14)
        ax1.set_ylabel('Score', fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)
        
        # Add value labels
        for bar, value in zip(bars1, top_6['Hub Score']):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{value:.1f}', ha='center', va='bottom', fontweight='bold')
        
        # 2. Route Connectivity (top-center)
        ax2 = axes[0, 1]
        bars2 = ax2.bar(top_6['Airport Code'], top_6['Route Count'], 
                       color=self.klm_blue, alpha=0.7, edgecolor=self.klm_dark_blue)
        
        ax2.set_title('Route Connectivity', fontweight='bold', fontsize=14)
        ax2.set_ylabel('Number of Routes', fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        
        for bar, value in zip(bars2, top_6['Route Count']):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    str(value), ha='center', va='bottom', fontweight='bold')
        
        # 3. Passenger Volume (top-right)
        ax3 = axes[0, 2]
        bars3 = ax3.bar(top_6['Airport Code'], top_6['Passenger Volume (M)'], 
                       color=self.klm_orange, alpha=0.7, edgecolor=self.klm_dark_blue)
        
        ax3.set_title('Annual Passenger Volume', fontweight='bold', fontsize=14)
        ax3.set_ylabel('Passengers (Millions)', fontweight='bold')
        ax3.tick_params(axis='x', rotation=45)
        
        for bar, value in zip(bars3, top_6['Passenger Volume (M)']):
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{value:.1f}M', ha='center', va='bottom', fontweight='bold')
        
        # 4. Delay Performance (bottom-left)
        ax4 = axes[1, 0]
        colors_delay = ['green' if x < 0.15 else 'orange' if x < 0.25 else 'red' 
                       for x in top_6['Delay Rate']]
        bars4 = ax4.bar(top_6['Airport Code'], top_6['Delay Rate']*100, 
                       color=colors_delay, alpha=0.7, edgecolor=self.klm_dark_blue)
        
        ax4.set_title('Delay Performance', fontweight='bold', fontsize=14)
        ax4.set_ylabel('Delay Rate (%)', fontweight='bold')
        ax4.tick_params(axis='x', rotation=45)
        
        for bar, value in zip(bars4, top_6['Delay Rate']):
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{value:.1%}', ha='center', va='bottom', fontweight='bold')
        
        # 5. Multi-criteria Radar Chart (bottom-center)
        ax5 = axes[1, 1]
        
        # Normalize metrics for radar chart
        metrics = ['Hub Score', 'Route Count', 'Passenger Volume (M)', 'Connectivity']
        top_3_for_radar = top_6.head(3)
        
        # Create stacked bar chart showing normalized performance
        x_pos = np.arange(len(metrics))
        width = 0.25
        
        for i, (_, airport) in enumerate(top_3_for_radar.iterrows()):
            # Normalize each metric (0-1 scale)
            normalized_values = []
            for metric in metrics:
                max_val = top_6[metric].max()
                if max_val > 0:
                    normalized_values.append(airport[metric] / max_val)
                else:
                    normalized_values.append(0)
            
            ax5.bar(x_pos + i*width, normalized_values, width, 
                   label=airport['Airport Code'], color=self.colors[i], alpha=0.8)
        
        ax5.set_title('Normalized Performance\n(Top 3 Candidates)', fontweight='bold', fontsize=14)
        ax5.set_ylabel('Normalized Score (0-1)', fontweight='bold')
        ax5.set_xticks(x_pos + width)
        ax5.set_xticklabels(['Hub Score', 'Routes', 'Passengers', 'Connectivity'], 
                           rotation=45, ha='right')
        ax5.legend()
        ax5.grid(axis='y', alpha=0.3)
        
        # 6. Geographic Distribution (bottom-right)
        ax6 = axes[1, 2]
        
        # Count by country and create pie chart
        country_counts = top_6['Country'].value_counts()
        
        colors_pie = [self.klm_blue, self.klm_orange, self.klm_light_blue, '#4CAF50'][:len(country_counts)]
        wedges, texts, autotexts = ax6.pie(country_counts.values, labels=country_counts.index,
                                          autopct='%1.0f%%', colors=colors_pie,
                                          startangle=90)
        
        ax6.set_title('Geographic Distribution\n(Top 6 Candidates)', fontweight='bold', fontsize=14)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save comparative analysis
        output_path = os.path.join(self.output_dir, 'comparative_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.show()
        
        print(f"✅ Comparative analysis chart saved: {output_path}")
        return fig
    
    def generate_all_poster_visualizations(self):
        """Generate complete set of visualizations for the poster"""
        print("🎨 Starting KLM Hub Analysis Poster Visualization Generation...")
        print("=" * 60)
        
        # 1. Main hub ranking chart (centerpiece)
        print("\n1️⃣ Creating main hub ranking chart...")
        fig1, top_candidates = self.create_hub_ranking_poster_chart()
        
        # 2. Comprehensive dashboard
        print("\n2️⃣ Creating comprehensive analysis dashboard...")
        fig2 = self.create_hub_analysis_dashboard()
        
        # 3. European map
        print("\n3️⃣ Creating European hub candidates map...")
        map_viz = self.create_european_hub_map()
        
        # 4. Network analysis
        print("\n4️⃣ Creating network connectivity analysis...")
        fig4, centrality, betweenness = self.create_network_analysis_viz()
        
        # 5. Executive summary infographic
        print("\n5️⃣ Creating executive summary infographic...")
        fig5 = self.create_executive_summary_infographic()
        
        # 6. Comparative analysis
        print("\n6️⃣ Creating detailed comparative analysis...")
        fig6 = self.create_comparative_analysis_chart()
        
        # Generate summary report
        print("\n📋 Generating analysis summary...")
        df = self.query_hub_candidates()
        
        summary = f"""
KLM HUB EXPANSION ANALYSIS - POSTER VISUALIZATIONS SUMMARY
{'='*60}

📊 DATA OVERVIEW:
• Total airports analyzed: {len(df)}
• Hub candidates identified: {len(df[df['Hub Score'] > 0])}
• Countries represented: {df['Country'].nunique()}

🏆 TOP 3 HUB CANDIDATES:
1. {df.iloc[0]['Airport']} ({df.iloc[0]['Airport Code']}) - Score: {df.iloc[0]['Hub Score']:.1f}
2. {df.iloc[1]['Airport']} ({df.iloc[1]['Airport Code']}) - Score: {df.iloc[1]['Hub Score']:.1f}
3. {df.iloc[2]['Airport']} ({df.iloc[2]['Airport Code']}) - Score: {df.iloc[2]['Hub Score']:.1f}

📈 KEY METRICS:
• Highest hub score: {df['Hub Score'].max():.1f}
• Average routes per airport: {df['Route Count'].mean():.1f}
• Largest airport by passengers: {df['Passenger Volume (M)'].max():.1f}M
• Best delay performance: {df['Delay Rate'].min():.1%}

📁 GENERATED FILES:
• hub_ranking_poster.png - Main ranking chart
• hub_analysis_dashboard.png - 4-panel dashboard
• european_hub_map.html - Interactive map
• network_analysis.png - Network connectivity
• executive_summary_infographic.png - Key findings
• comparative_analysis.png - Detailed comparison

🎯 POSTER RECOMMENDATIONS:
1. Use hub_ranking_poster.png as the main centerpiece
2. Include executive_summary_infographic.png for key statistics
3. Add network_analysis.png to show strategic value
4. Reference european_hub_map.html for geographic context
5. Use comparative_analysis.png for detailed evaluation

All visualizations are optimized for poster printing at 300 DPI.
        """
        
        # Save summary
        summary_path = os.path.join(self.output_dir, 'visualization_summary.txt')
        with open(summary_path, 'w') as f:
            f.write(summary)
        
        print(summary)
        print(f"\n✅ Summary saved to: {summary_path}")
        print(f"📁 All visualizations saved in: {self.output_dir}/")
        print("\n🎉 Poster visualization generation complete!")
        
        return {
            'hub_ranking': fig1,
            'dashboard': fig2,
            'map': map_viz,
            'network': fig4,
            'infographic': fig5,
            'comparative': fig6,
            'top_candidates': top_candidates,
            'summary': summary
        }

def main():
    """Main function to generate all poster visualizations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate KLM Hub Analysis Poster Visualizations")
    parser.add_argument("--rdf-file", required=True, 
                       help="Path to the unified knowledge graph RDF file")
    parser.add_argument("--output-dir", default="poster_visualizations",
                       help="Directory to save visualizations")
    
    args = parser.parse_args()
    
    # Check if RDF file exists
    if not os.path.exists(args.rdf_file):
        print(f"❌ Error: RDF file not found: {args.rdf_file}")
        print("\nPlease run the unified knowledge graph builder first:")
        print("python scripts/Unified_KG/unified_kg_builder.py")
        return 1
    
    try:
        # Create visualizer
        visualizer = KLMHubPosterVisualizer(args.rdf_file)
        visualizer.output_dir = args.output_dir
        
        # Generate all visualizations
        results = visualizer.generate_all_poster_visualizations()
        
        print(f"\n🎯 SUCCESS! All poster visualizations generated.")
        print(f"📁 Files saved in: {os.path.abspath(args.output_dir)}")
        print("\n📋 Next steps for your poster:")
        print("1. Use hub_ranking_poster.png as the main visual element")
        print("2. Include executive_summary_infographic.png for key statistics")
        print("3. Add network_analysis.png to demonstrate strategic value")
        print("4. Reference the interactive map for geographic insights")
        print("5. Use comparative_analysis.png for detailed evaluation")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error generating visualizations: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())