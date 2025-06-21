import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

def main():
    # Load the final integrated dataset
    df = pd.read_csv(Path.cwd() / "data" / "new_rq3" / "final_dataset.csv")
    
    # Pick the top N delayed airports by Avg_ATC_Delay_2023
    N = 5
    topN = df.nlargest(N, 'Avg_ATC_Delay_2023')
    
    # Build a directed graph
    G = nx.DiGraph()
    for _, row in topN.iterrows():
        iata    = row['IATA_Code']
        country = row['ISO_Country']
        cls     = 'HubAirport' if row['HubStatus']=='Hub' else 'RegionalAirport'
        
        # Add nodes with colors
        G.add_node(iata,    label=iata,    color='#FFA500')  # orange
        G.add_node(country, label=country, color='#ADD8E6')  # lightblue
        G.add_node(cls,     label=cls,     color='#90EE90')  # lightgreen
        
        # Add edges
        G.add_edge(iata, country, label='locatedIn')
        G.add_edge(iata, cls,     label='rdf:type')
    
    # Layout and draw
    pos = nx.spring_layout(G, k=1.0, iterations=100)
    node_colors = [data['color'] for _, data in G.nodes(data=True)]
    labels      = nx.get_node_attributes(G, 'label')
    edge_labels = nx.get_edge_attributes(G, 'label')
    
    plt.figure(figsize=(8, 6))
    nx.draw(G, pos,
            labels=labels,
            node_color=node_colors,
            node_size=1200,
            font_size=10,
            font_weight='bold',
            arrows=True,
            arrowstyle='-|>')
    nx.draw_networkx_edge_labels(G, pos,
            edge_labels=edge_labels,
            font_color='red',
            font_size=8)
    plt.title(f"Top {N} Delayed Schiphol Destinations (KG Excerpt)")
    plt.tight_layout()
    
    # Save
    out = Path.cwd() / "data" / "new_rq3" / "plots" / f"kg_top{N}_excerpt.png"
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"✔ Saved KG excerpt to {out}")

if __name__ == "__main__":
    main()