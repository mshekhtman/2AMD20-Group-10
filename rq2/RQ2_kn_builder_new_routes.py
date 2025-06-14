import pandas as pd
from rdflib import Graph, Literal, RDF, Namespace, URIRef
from rdflib.namespace import XSD, RDFS

# === Namespaces ===
EX = Namespace("http://example.org/klm#")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

# === Initialize RDF Graph ===
g = Graph()
g.bind("ex", EX)
g.bind("geo", GEO)

# === Load Datasets ===

# 1. Filtered Airports (with IATA)
airports = pd.read_csv("rq2/datasets/filtered_airports.csv")
airports = airports[~airports["iata_code"].isnull()]

# 2. Routes
all_routes = pd.read_csv("rq2/datasets/airline_routes.csv")

# 3. Eurostat Passenger Numbers
passengers = pd.read_csv("eurostat_data/processed/estat_avia_paoac_airport_totals.csv")
year = "2024"
if year not in passengers.columns:
    raise ValueError(f"Year column '{year}' not found in passenger data")
passenger_dict = dict(zip(passengers["icao"], passengers[year]))

# 4. Cities
cities = pd.read_csv("rq2/datasets/worldcities.csv")
city_dict = {}

# === Add City Nodes ===

for _, row in cities.iterrows():
    city_name = str(row["city"]).strip()
    country = str(row["iso2"]).strip()
    city_key = f"{city_name}_{country}".replace(" ", "_").replace("`", "")
    city_uri = EX["City_" + city_key]

    if city_key not in city_dict:
        city_dict[city_key] = city_uri
        g.add((city_uri, RDF.type, EX.City))
        g.add((city_uri, RDFS.label, Literal(city_name)))
        g.add((city_uri, EX.country, Literal(country)))
        g.add((city_uri, GEO.lat, Literal(row["lat"], datatype=XSD.float)))
        g.add((city_uri, GEO.long, Literal(row["lng"], datatype=XSD.float)))
        if not pd.isna(row["population"]):
            g.add((city_uri, EX.population, Literal(int(row["population"]), datatype=XSD.integer)))

# === Add Airport Nodes ===

# Build continent map from airline_routes.csv (all_routes)
continent_map = all_routes.dropna(subset=["airport_iata", "continent"]) \
                          .drop_duplicates(subset=["airport_iata"]) \
                          .set_index("airport_iata")["continent"].to_dict()

airport_uri_map = {}

for _, row in airports.iterrows():
    iata = str(row["iata_code"]).strip()
    icao = str(row["airport_ident"]).strip()

    if not iata or iata.upper() == "\\N":
        continue

    uri = EX[f"Airport_{iata}_{icao}"]
    airport_uri_map[(iata, icao)] = uri

    g.add((uri, RDF.type, EX.Airport))
    g.add((uri, EX.iataCode, Literal(iata)))
    g.add((uri, EX.icaoCode, Literal(icao)))
    g.add((uri, RDFS.label, Literal(row["name"])))

    # Add continent info from airline_routes.csv if available
    if iata in continent_map:
        g.add((uri, EX.locatedInContinent, Literal(continent_map[iata])))

    city_name = str(row["municipality"]).strip()
    country = str(row["iso_country"]).strip()
    city_key = f"{city_name}_{country}".replace(" ", "_").replace("`", "")

    if city_key in city_dict:
        g.add((uri, EX.locatedInCity, city_dict[city_key]))
    else:
        g.add((uri, EX.city, Literal(city_name)))

    g.add((uri, EX.country, Literal(country)))
    g.add((uri, GEO.lat, Literal(row["latitude_deg"], datatype=XSD.float)))
    g.add((uri, GEO.long, Literal(row["longitude_deg"], datatype=XSD.float)))

    if icao in passenger_dict:
        g.add((uri, EX.hasPassengerCount, Literal(int(passenger_dict[icao]), datatype=XSD.integer)))

# === Add Routes (New Format) ===

for _, row in all_routes.iterrows():
    source_iata = str(row["airport_iata"]).strip()
    dest_iata = str(row["dest_iata"]).strip()
    airline = str(row["carrier_iata"]).strip().upper()

    # Look up ICAOs from airports table
    source_icao_vals = airports.loc[airports["iata_code"] == source_iata, "airport_ident"].values
    dest_icao_vals = airports.loc[airports["iata_code"] == dest_iata, "airport_ident"].values

    if len(source_icao_vals) == 0 or len(dest_icao_vals) == 0 or not airline:
        continue

    source_icao = source_icao_vals[0]
    dest_icao = dest_icao_vals[0]
    source_key = (source_iata, source_icao)
    dest_key = (dest_iata, dest_icao)

    if source_key not in airport_uri_map or dest_key not in airport_uri_map:
        continue

    route_uri = EX[f"Route_{source_iata}_{dest_iata}_{airline}"]
    g.add((route_uri, RDF.type, EX.FlightRoute))
    g.add((route_uri, EX.sourceAirport, airport_uri_map[source_key]))
    g.add((route_uri, EX.destinationAirport, airport_uri_map[dest_key]))
    g.add((route_uri, EX.operatedBy, Literal(airline)))

    if airline == "KL":
        g.add((route_uri, EX.operatedByKLM, Literal(True, datatype=XSD.boolean)))

# === Serialize RDF Graph ===

g.serialize(destination="rq2/knowledge_graphs/rq2_new_routes_v2.rdf", format="xml")
print("RDF graph successfully saved to rq2_new_routes_v2.rdf")
