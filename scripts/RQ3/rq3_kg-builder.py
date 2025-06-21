import pandas as pd
from rdflib import Graph, Namespace, Literal, RDF, XSD
from pathlib import Path

df = pd.read_csv(Path.cwd() / "data/new_rq3/final_dataset.csv")

EX = Namespace("http://example.org/airport#")
g = Graph()
g.bind("ex", EX)

for _, r in df.iterrows():
    uri = EX[r['Dest_ICAO']]
    g.add((uri, RDF.type, EX.Airport))
    # Data props
    g.add((uri, EX.icaoCode, Literal(r['Dest_ICAO'], datatype=XSD.string)))
    if pd.notna(r['IATA_Code']):
        g.add((uri, EX.iataCode, Literal(r['IATA_Code'], datatype=XSD.string)))
    g.add((uri, EX.hasFlights, Literal(int(r['Flights_2023']), datatype=XSD.integer)))
    g.add((uri, EX.hasPassengers, Literal(int(r['Passengers_2023']), datatype=XSD.integer)))
    g.add((uri, EX.hasAvgATCDelay, Literal(float(r['Avg_ATC_Delay_2023']),datatype=XSD.float)))
    # Geo
    for prop, col in [(EX.lat,'Latitude'),(EX.long,'Longitude')]:
        if pd.notna(r[col]):
            g.add((uri, prop, Literal(float(r[col]), datatype=XSD.float)))
    # Runway
    if pd.notna(r['LongestRunwayLength']):
        g.add((uri, EX.runwayLength, Literal(int(r['LongestRunwayLength']),datatype=XSD.integer)))
    if pd.notna(r['LongestRunwaySurface']):
        g.add((uri, EX.runwaySurface, Literal(r['LongestRunwaySurface'],datatype=XSD.string)))
    # Country
    if pd.notna(r['ISO_Country']):
        c = EX[f"Country_{r['ISO_Country']}"]
        g.add((c, RDF.type, EX.Country))
        g.add((c, EX.isoCode, Literal(r['ISO_Country'],datatype=XSD.string)))
        g.add((uri, EX.locatedIn, c))
    # Classification
    cls = EX.HubAirport if r['HubStatus']=="Hub" else EX.RegionalAirport
    g.add((uri, RDF.type, cls))

out = Path.cwd() / "data/new_rq3/rq3_knowledge_graph.ttl"
g.serialize(out, format="turtle")
print(f"KG written with {len(g)} triples")
