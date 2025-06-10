#!/usr/bin/env python3
import os
import pandas as pd
import networkx as nx
from pyvis.network import Network
from rdflib import Graph, Namespace
from rdflib.namespace import RDF
import matplotlib as mpl

# Namespaces
EX = Namespace("http://example.org/flight/")

# Load the KG
g = Graph().parse("outputs/flight_delay_kg.ttl", format="turtle")

# Build a simple graph: airports only
G = nx.Graph()
for s, p, o in g.triples((None, EX.hasDestination, None)):
    airport = str(o).split("/")[-1]
    G.add_node(airport)

# Load analysis table for attributes
df = pd.read_csv("data/processed/analysis_table.csv").set_index("destinationIATA")

# Attach attributes
for node, data in G.nodes(data=True):
    if node in df.index:
        nf = df.at[node, "num_flights"]
        ad = df.at[node, "avg_delay"]
        if hasattr(nf, "item"):
            nf = nf.item()
        if hasattr(ad, "item"):
            ad = ad.item()
        data["numFlights"] = int(nf)
        data["avgDelay"]   = float(ad)

# Create PyVis network
net = Network(height="750px", width="100%", bgcolor="#ffffff", notebook=False)
net.from_nx(G)

# Style nodes
max_f = df["num_flights"].max()
cmap = mpl.colormaps["coolwarm"]
vmax = df["avg_delay"].max()

for node in net.nodes:
    nf = node.get("numFlights", 1)
    ad = node.get("avgDelay", 0.0)

    # size by flight count
    node["value"] = 5 + (nf / max_f) * 30

    # color by avg delay
    norm_val = ad / vmax if vmax else 0
    r, g_, b, _ = cmap(norm_val)
    node["color"] = f"rgb({int(255*r)},{int(255*g_)},{int(255*b)})"

    node["title"] = (
        f"{node['id']}<br>"
        f"Flights: {nf}<br>"
        f"Avg Delay: {ad:.1f} min"
    )

# Improve layout physics
net.force_atlas_2based(
    gravity=-50,
    central_gravity=0.005,
    spring_length=200,
    spring_strength=0.08,
    damping=0.4
)

# Save interactive HTML (no notebook rendering)
os.makedirs("outputs", exist_ok=True)
net.write_html("outputs/airport_only_kg.html")
print("✅ Interactive KG viz saved to outputs/airport_only_kg.html")
