"""
KLM Hub Network Visualization - Individual Script

Creates network visualization showing current vs potential hub networks.
Shows the strategic value of adding a second hub.
"""

import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from rdflib import Graph
import numpy as np
import os
import sys

class HubNetworkViz:
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
    
    def query_route_network(self):
        """Query knowledge graph for route network data"""
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
        LIMIT 100
        """
        
        results = self.graph.query(route_query)
        
        routes = []
        for row in results:
            origin = str(row.originName) if row.originName else str(row.origin).split('/')[-1]
            dest = str(row.destName) if row.destName else str(row.destination).split('/')[-1]
            routes.append((origin, dest))
        
        return routes
    
    def query_hub_candidates(self):
        """Query for top hub candidates"""
        query = """
        PREFIX klm: <http://example.org/klm/>
        
        SELECT ?airport ?name ?hubScore ?routeCount
        WHERE {
            ?airport a klm:Airport ;
                    klm:name ?name ;
                    klm:hubPotentialScore ?hubScore ;
                    klm:routeCount ?routeCount .
            
            FILTER NOT EXISTS { ?airport klm:isMainHub true }
            FILTER(?routeCount >= 5)
        }
        ORDER BY DESC(?hubScore)
        LIMIT 5
        """
        
        results = self.graph.query(query)
        candidates = []
        
        for row in results:
            candidates.append({
                'name': str(row.name) if row.name else 'Unknown',
                'hub_score': float(row.hubScore) if row.hubScore else 0,
                'route_count': int(row.routeCount) if row.routeCount else 0
            })
        
        return candidates
    
    def create_network_visualization(self):
        """Create network visualization comparing current vs potential networks"""
        print("🔍 Querying knowledge graph for network data...")
        routes = self.query_route_network()
        candidates = self.query_hub_candidates()
        
        if len(routes) == 0:
            print("❌ No route data found")
            return None
        
        print(f"📊 Found {len(routes)} routes for network analysis")
        
        # Create NetworkX graph from routes
        G = nx.Graph()
        for origin, dest in routes:
            G.add_edge(origin, dest)
        
        # Calculate centrality measures
        centrality = nx.degree_centrality(G)
        betweenness = nx.betweenness_centrality(G)
        
        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
        
        # Current network (left panel)
        print("📈 Creating current network visualization...")
        pos1 = nx.spring_layout(G, k=3, iterations=50, seed=42)
        
        # Node sizes based on centrality
        node_sizes1 = [centrality[node] * 2000 + 100 for node in G.nodes()]
        node_colors1 = [centrality[node] for node in G.nodes()]
        
        # Draw current network
        nx.draw_networkx_nodes(G, pos1, node_size=node_sizes1, 
                              node_color=node_colors1, cmap=plt.cm.Blues,
                              alpha=0.8, ax=ax1)
        nx.draw_networkx_edges(G, pos1, alpha=0.2, width=0.5, ax=ax1)
        
        # Label major hubs (top 8 by centrality)
        major_hubs = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:8]
        labels1 = {}
        for node, cent in major_hubs:
            # Shorten long airport names for display
            display_name = node.replace('Airport', '').replace('International', '').strip()
            if len(display_name) > 15:
                display_name = display_name[:12] + '...'
            labels1[node] = display_name
        
        nx.draw_networkx_labels(G, pos1, labels1, font_size=9, 
                               font_weight='bold', ax=ax1)
        
        ax1.set_title('Current Route Network\n(Node size = Connectivity)', 
                     fontweight='bold', fontsize=16)
        ax1.axis('off')
        
        # Potential network with second hub (right panel)
        print("🚀 Creating potential network with second hub...")
        
        if len(candidates) > 0:
            second_hub_name = candidates[0]['name']
            
            # Create enhanced network
            G_potential = G.copy()
            
            # Add theoretical connections from second hub to major destinations
            major_destinations = [node for node, cent in major_hubs[:10]]
            
            # Add connections if second hub exists in network
            hub_found = False
            for node in G_potential.nodes():
                if second_hub_name.lower() in node.lower() or any(word in node.lower() for word in second_hub_name.lower().split()):
                    hub_found = True
                    second_hub_node = node
                    break
            
            if not hub_found:
                # Add second hub as new node if not found
                second_hub_node = second_hub_name
                G_potential.add_node(second_hub_node)
            
            # Add strategic connections
            for dest in major_destinations[:8]:
                if dest != second_hub_node:
                    G_potential.add_edge(second_hub_node, dest)
            
            # Recalculate centrality for enhanced network
            new_centrality = nx.degree_centrality(G_potential)
            
            # Create layout for potential network
            pos2 = pos1.copy()
            if second_hub_node not in pos2:
                # Position new hub strategically
                pos2[second_hub_node] = (0.3, 0.3)
            
            # Update node sizes and colors
            node_sizes2 = [new_centrality.get(node, 0) * 2000 + 100 for node in G_potential.nodes()]
            
            # Special colors for hubs
            special_colors = []
            for node in G_potential.nodes():
                if node == second_hub_node:
                    special_colors.append(1.0)  # Max color for second hub
                elif 'amsterdam' in node.lower() or 'schiphol' in node.lower():
                    special_colors.append(0.8)  # High color for current hub
                else:
                    special_colors.append(new_centrality.get(node, 0))
            
            # Draw enhanced network
            nx.draw_networkx_nodes(G_potential, pos2, node_size=node_sizes2,
                                  node_color=special_colors, cmap=plt.cm.Reds,
                                  alpha=0.8, ax=ax2)
            nx.draw_networkx_edges(G_potential, pos2, alpha=0.2, width=0.5, ax=ax2)
            
            # Highlight new connections from second hub
            new_edges = [(second_hub_node, dest) for dest in major_destinations[:8] 
                        if dest != second_hub_node and G_potential.has_edge(second_hub_node, dest)]
            
            nx.draw_networkx_edges(G_potential, pos2, edgelist=new_edges, 
                                  edge_color=self.klm_orange, width=2, alpha=0.8, ax=ax2)
            
            # Label both hubs
            labels2 = {}
            for node in G_potential.nodes():
                if node == second_hub_node:
                    display_name = second_hub_name.replace('Airport', '').strip()
                    if len(display_name) > 15:
                        display_name = display_name[:12] + '...'
                    labels2[node] = f"{display_name}\n(New Hub)"
                elif 'amsterdam' in node.lower() or 'schiphol' in node.lower():
                    labels2[node] = 'Amsterdam\n(Current Hub)'
            
            nx.draw_networkx_labels(G_potential, pos2, labels2, font_size=9,
                                   font_weight='bold', ax=ax2)
            
            ax2.set_title(f'Enhanced Network with Second Hub\n(Orange edges = New connections)', 
                         fontweight='bold', fontsize=16)
        else:
            ax2.text(0.5, 0.5, 'No hub candidates found', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=16)
            ax2.set_title('Enhanced Network Analysis', fontweight='bold', fontsize=16)
        
        ax2.axis('off')
        
        # Overall title
        fig.suptitle('NETWORK CONNECTIVITY ANALYSIS: CURRENT vs POTENTIAL HUB EXPANSION', 
                    fontsize=20, fontweight='bold', y=0.95, color=self.klm_dark_blue)
        
        # Add subtitle with key insights
        if len(candidates) > 0:
            subtitle = f"Strategic Analysis: Adding {candidates[0]['name']} as Second Hub"
            fig.text(0.5, 0.91, subtitle, ha='center', fontsize=14, 
                    style='italic', color=self.klm_gray)
        
        plt.tight_layout()
        plt.subplots_adjust(top=0.87)
        
        # Save network analysis
        output_path = os.path.join(self.output_dir, 'hub_network_analysis.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        
        print(f"✅ Network analysis saved: {output_path}")
        
        # Show plot
        plt.show()
        
        # Print network statistics
        print(f"\n📊 NETWORK ANALYSIS:")
        print(f"Total airports in network: {len(G.nodes())}")
        print(f"Total routes: {len(G.edges())}")
        print(f"Network density: {nx.density(G):.3f}")
        
        if len(candidates) > 0:
            print(f"Top hub candidate: {candidates[0]['name']}")
            print(f"Hub score: {candidates[0]['hub_score']:.1f}")
            print(f"Current routes: {candidates[0]['route_count']}")
        
        # Print top airports by centrality
        print(f"\nTop 5 airports by connectivity:")
        for i, (airport, cent) in enumerate(major_hubs[:5]):
            display_name = airport.replace('Airport', '').replace('International', '').strip()
            print(f"{i+1}. {display_name}: {cent:.3f}")
        
        return fig, G, centrality

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python hub_network_viz.py <rdf_file_path>")
        print("Example: python hub_network_viz.py data/knowledge_graph/unified_kg.ttl")
        return 1
    
    rdf_file = sys.argv[1]
    
    if not os.path.exists(rdf_file):
        print(f"❌ RDF file not found: {rdf_file}")
        return 1
    
    try:
        print("🌐 Creating KLM Hub Network Visualization...")
        network_viz = HubNetworkViz(rdf_file)
        fig, graph, centrality = network_viz.create_network_visualization()
        
        if fig is not None:
            print("🎉 Network visualization created successfully!")
            print("📁 Check the 'poster_visualizations' folder for the output.")
        else:
            print("❌ Failed to create network visualization - no data found")
            return 1
            
    except Exception as e:
        print(f"❌ Error creating network visualization: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())