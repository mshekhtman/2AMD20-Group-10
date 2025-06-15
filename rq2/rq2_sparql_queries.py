#Airports KLM does not serve, but which still have high passenger counts
""""
PREFIX ex: <http://example.org/klm#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?airport ?iata ?cityName ?passengers
WHERE {
  GRAPH <http://example.org/graph1> {
    ?airport a ex:Airport ;
             ex:iataCode ?iata ;
             ex:hasPassengerCount ?passengers ;
             ex:locatedInCity ?city .

    ?city rdfs:label ?cityName .

    FILTER NOT EXISTS {
      ?route a ex:FlightRoute ;
             ex:destinationAirport ?airport ;
             ex:operatedByKLM true .
    }

    FILTER(?passengers > 1000000)
  }
}
ORDER BY DESC(?passengers)
LIMIT 10
"""


# Airports KLM Does Not Fly To, But Are Near High-Population Cities
"""
PREFIX ex: <http://example.org/klm#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?airport ?iata ?icao ?cityName ?population
WHERE {
  GRAPH <http://example.org/graph1> {
    ?airport a ex:Airport ;
             ex:iataCode ?iata ;
             ex:icaoCode ?icao .

    # Linked city (structured)
    OPTIONAL {
      ?airport ex:locatedInCity ?city .
      ?city rdfs:label ?linkedCityName ;
            ex:population ?population .
    }

    # Fallback to city literal if structured city is missing
    OPTIONAL {
      ?airport ex:city ?cityLiteral .
    }

    # Use structured city name if available, else fallback to literal
    BIND(COALESCE(?linkedCityName, ?cityLiteral) AS ?cityName)

    # Ensure no KLM route to this airport
    FILTER NOT EXISTS {
      ?route a ex:FlightRoute ;
             ex:destinationAirport ?airport ;
             ex:operatedByKLM true .
    }

    # Only show cities with significant population
    FILTER(?population > 1000000)
  }
}
ORDER BY DESC(?population)
LIMIT 20
"""



### OTHER TESTING QUERIES ###
# Airports KLM Does Not Serve, But Are in High-Population Cities in Europe
"""
PREFIX ex: <http://example.org/klm#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?airport ?iata ?icao ?cityName ?population
WHERE {
  GRAPH <http://example.org/graph2> {
    ?airport a ex:Airport ;
             ex:iataCode ?iata ;
             ex:icaoCode ?icao ;
             ex:locatedInContinent "EU" ;
             ex:locatedInCity ?city .

    ?city rdfs:label ?cityName ;
          ex:population ?population .

    # Airport is not served by KLM
    FILTER NOT EXISTS {
      ?route a ex:FlightRoute ;
             ex:destinationAirport ?airport ;
             ex:operatedByKLM true .
    }

    # Exclude cities that already have at least one airport served by KLM
    FILTER NOT EXISTS {
      ?klmAirport ex:locatedInCity ?city .
      ?route2 a ex:FlightRoute ;
              ex:destinationAirport ?klmAirport ;
              ex:operatedByKLM true .
    }

    FILTER(?population > 500000)
  }
}
ORDER BY DESC(?population)
LIMIT 30
"""
