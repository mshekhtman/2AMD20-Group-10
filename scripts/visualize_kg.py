#!/usr/bin/env python3
import os
import pandas as pd
import networkx as nx
from pyvis.network import Network
from rdflib import Graph, Namespace
from rdflib.namespace import RDF
import matplotlib as mpl
import numpy as np

EX = Namespace("http://example.org/flight/")

# --- Build and style the network ---
def build_pyvis_network():
    # 1) Load KG
    g = Graph().parse("outputs/flight_delay_kg.ttl", format="turtle")
    g.bind("ex", EX)

    # 2) Build a simple graph: airports + region nodes
    G = nx.Graph()

    # Load airport summary for attributes
    df = pd.read_csv("data/processed/analysis_table.csv").set_index("destinationIATA")

    # Add airport nodes
    for airport, row in df.iterrows():
        G.add_node(airport, 
                   group="airport", 
                   numFlights=int(row.num_flights),
                   avgDelay=float(row.avg_delay))

    # Add region nodes and edges
    arc = pd.read_csv("data/ArcGIS/ArcGIS_data.csv", low_memory=False)
    arc = arc.rename(columns={"IATA-Code":"destinationIATA","ISO-Country":"iso_country"})
    arc = arc[["destinationIATA","iso_country"]].dropna()
    regions = arc["iso_country"].unique()
    for reg in regions:
        G.add_node(reg, group="region")

    for _, row in arc.iterrows():
        ap = row.destinationIATA
        reg = row.iso_country
        if ap in G and reg in G:
            G.add_edge(ap, reg)

    # 3) PyVis network
    net = Network(height="750px", width="100%", bgcolor="#ffffff", notebook=False)
    net.force_atlas_2based(gravity=-50, spring_length=200, spring_strength=0.08, damping=0.4)
    net.from_nx(G)

    # 4) Style nodes
    max_f = df["num_flights"].max()
    vmax  = df["avg_delay"].max()
    cmap  = mpl.colormaps["coolwarm"]
    for node in net.nodes:
        grp = node["group"]
        if grp == "airport":
            nf = node["numFlights"]
            ad = node["avgDelay"]
            # size ∝ flight count
            node["value"] = 5 + (nf/max_f)*30
            # color ∝ avg delay
            c = cmap(ad/vmax)
            node["color"] = f"rgb({int(255*c[0])},{int(255*c[1])},{int(255*c[2])})"
            node["title"] = f"{node['id']}<br>Flights: {nf}<br>Avg Delay: {ad:.1f} min"
        else:
            # region node
            node["value"] = 15
            node["color"] = "#888888"
            node["title"] = f"Region: {node['id']}"

    return net

# --- Inject legend into HTML ---
def add_legend_to_html(src_html, dst_html):
    legend_html = """
    <div style="position:absolute; bottom:20px; left:20px; background:white; 
                padding:10px; border:1px solid black; z-index:999;">
      <b>Legend</b><br>
      <svg height="10" width="10"><circle cx="5" cy="5" r="5" fill="#440154" /></svg> High delay<br>
      <svg height="10" width="10"><circle cx="5" cy="5" r="5" fill="#21908C" /></svg> Low delay<br>
      <div style="margin-top:5px;"></div>
      <b>Size ∝ Flight Count</b>
    </div>
    """
    html = open(src_html, 'r', encoding='utf8').read()
    idx = html.find("<body>")
    if idx==-1:
        combined = legend_html + html
    else:
        combined = html[:idx+6] + legend_html + html[idx+6:]
    with open(dst_html, 'w', encoding='utf8') as f:
        f.write(combined)
    print(f"✅ Legend injected into {dst_html}")

# --- Main driver ---
if __name__=="__main__":
    os.makedirs("outputs", exist_ok=True)
    net = build_pyvis_network()
    
    # 1) Write base HTML
    base = "outputs/airport_only_kg.html"
    net.write_html(base)
    print(f"⚡ Base KG viz: {base}")

    # 2) Write HTML with legend
    with_legend = "outputs/airport_only_kg_with_legend.html"
    add_legend_to_html(base, with_legend)
