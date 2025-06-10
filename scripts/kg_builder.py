#!/usr/bin/env python3
import os
import json
import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
from pyshacl import validate
from dateutil import parser

EX = Namespace("http://example.org/flight/")
g  = Graph()

def build_kg():
    # 1) Load analysis table (airport-level)
    df_air = pd.read_csv("data/processed/analysis_table.csv")

    # 2) Load raw ArcGIS CSV for ISO-Country & runway (for region mapping)
    arc = pd.read_csv("data/ArcGIS/ArcGIS_data.csv", low_memory=False)
    arc = arc.rename(columns={
        "IATA-Code":"destinationIATA",
        "ISO-Country":"iso_country"
    })
    arc = arc[["destinationIATA","iso_country"]].dropna()

    # 3) Build Airport nodes with basic metrics
    g.bind("ex", EX)
    for _, row in df_air.iterrows():
        a = EX[f"airport/{row.destinationIATA}"]
        g.add((a, RDF.type, EX.Airport))
        g.add((a, EX.numFlights, Literal(int(row.num_flights), datatype=XSD.integer)))
        g.add((a, EX.avgDelay,   Literal(float(row.avg_delay), datatype=XSD.double)))
        g.add((a, EX.isHub,      Literal(bool(row.isHub), datatype=XSD.boolean)))
        g.add((a, EX.isIntraEU,  Literal(bool(row.isIntraEU), datatype=XSD.boolean)))

    # 4) Region nodes & hasRegion links
    # Use ISO-Country as region identifier
    iso_codes = set(arc["iso_country"])
    for code in iso_codes:
        rnode = EX[f"region/{code}"]
        g.add((rnode, RDF.type, EX.Region))
    # Link each airport to its region
    merged = df_air[["destinationIATA"]].merge(arc, on="destinationIATA", how="left")
    for _, row in merged.iterrows():
        a = EX[f"airport/{row.destinationIATA}"]
        rc = row.iso_country
        if pd.isna(rc): 
            continue
        rnode = EX[f"region/{rc}"]
        g.add((a, EX.hasRegion, rnode))

    # 5) Load flight-level CSV to add Flight nodes & dayOfWeek
    flights = pd.read_csv("data/KLM/processed/flights_Q1_2025.csv")
    for idx, row in flights.iterrows():
        f = EX[f"flight/{idx}"]
        g.add((f, RDF.type, EX.Flight))
        g.add((f, EX.delayMinutes, Literal(float(row.delayMinutes), datatype=XSD.double)))
        # link flight → airport
        dest = row.destinationIATA
        if pd.notna(dest):
            g.add((f, EX.hasDestination, EX[f"airport/{dest}"]))
        # day of week
        dow = parser.isoparse(row.flightDate).strftime("%A")
        g.add((f, EX.dayOfWeek, Literal(dow, datatype=XSD.string)))

    # 6) Volume buckets: robustly create tertiles or equal‐width bins
    counts = df_air["num_flights"]
    try:
        # Attempt tertiles, dropping duplicate cuts if any
        df_air["bucket"] = pd.qcut(
            counts,
            q=3,
            labels=["Low", "Med", "High"],
            duplicates="drop"
        )
    except ValueError:
        # Fallback: equal‐width bins if qcut still fails
        df_air["bucket"] = pd.cut(
            counts,
            bins=3,
            labels=["Low", "Med", "High"]
        )

    # Create VolumeBucket class nodes
    for label in df_air["bucket"].cat.categories:
        vb = EX[f"volume/{label}"]
        g.add((vb, RDF.type, EX.VolumeBucket))

    # Link each airport to its bucket
    for _, row in df_air.iterrows():
        bucket = row["bucket"]
        if pd.isna(bucket):
            continue
        a = EX[f"airport/{row.destinationIATA}"]
        g.add((a, EX.inBucket, EX[f"volume/{bucket}"]))

    # 7) Validate against extended SHACL
    conforms, results, report = validate(
        g,
        shacl_graph="ontology/shapes.ttl",
        inference="rdfs",
        abort_on_first=False
    )
    if not conforms:
        print(report)
        raise RuntimeError("SHACL validation failed")

    # 8) Serialize the KG
    os.makedirs("outputs", exist_ok=True)
    out = "outputs/flight_delay_kg.ttl"
    g.serialize(out, format="turtle")
    print(f"✅ KG built: {out}")

if __name__ == "__main__":
    build_kg()
