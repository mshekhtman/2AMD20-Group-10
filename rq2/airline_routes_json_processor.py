import json
import csv

# Load JSON data from file
with open("rq2/datasets/airline_routes.json") as f:
    data = json.load(f)

# Define the CSV header
header = [
    "airport_iata", "airport_name", "city_name", "country", "country_code",
    "continent", "elevation", "latitude", "longitude", "icao",
    "timezone", "display_name",
    "dest_iata", "km", "min", "carrier_iata", "carrier_name"
]

# Open CSV for writing
with open("rq2/datasets/airline_routes.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)

    # Loop through airports
    for airport in data.values():
        base_info = [
            airport.get("iata"),
            airport.get("name"),
            airport.get("city_name"),
            airport.get("country"),
            airport.get("country_code"),
            airport.get("continent"),
            airport.get("elevation"),
            airport.get("latitude"),
            airport.get("longitude"),
            airport.get("icao"),
            airport.get("timezone"),
            airport.get("display_name"),
        ]
        
        # Loop through each route
        for route in airport.get("routes", []):
            if not route["carriers"]:
                # No carrier listed
                row = base_info + [
                    route.get("iata"),
                    route.get("km"),
                    route.get("min"),
                    "", ""  # No carrier
                ]
                writer.writerow(row)
            else:
                for carrier in route["carriers"]:
                    row = base_info + [
                        route.get("iata"),
                        route.get("km"),
                        route.get("min"),
                        carrier.get("iata"),
                        carrier.get("name")
                    ]
                    writer.writerow(row)
