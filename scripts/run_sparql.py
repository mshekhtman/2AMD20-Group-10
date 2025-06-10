#!/usr/bin/env python3
import os
import pandas as pd
from rdflib import Graph, Namespace
from rdflib.namespace import XSD

EX = Namespace("http://example.org/flight/")

def sparql_to_df(g, query, cols):
    res = g.query(query)
    # Boolean ASK?
    if isinstance(res, bool):
        return pd.DataFrame([{cols[0]: res}])
    rows = []
    for row in res:
        if isinstance(row, bool):
            continue
        rows.append(tuple(str(v) for v in row))
    return pd.DataFrame(rows, columns=cols)

def main():
    # Load KG
    g = Graph().parse("outputs/flight_delay_kg.ttl", format="turtle")
    g.bind("ex", EX)

    # 1) Summary per destination
    q1 = """
    PREFIX ex: <http://example.org/flight/>
    SELECT ?dest ?numFlights ?avgDelay ?isHub ?isIntraEU
    WHERE {
      ?airport a ex:Airport ;
               ex:numFlights ?numFlights ;
               ex:avgDelay   ?avgDelay ;
               ex:isHub      ?isHub ;
               ex:isIntraEU  ?isIntraEU .
      BIND(STRAFTER(STR(?airport),"airport/") AS ?dest)
    }
    ORDER BY DESC(xsd:integer(?numFlights))
    """
    df_sum = sparql_to_df(g, q1, ["destinationIATA","num_flights","avg_delay","isHub","isIntraEU"])
    df_sum = df_sum.assign(
        num_flights=df_sum.num_flights.astype(int),
        avg_delay=df_sum.avg_delay.astype(float),
        isHub=lambda x: x.isHub.map({"true":True,"false":False}),
        isIntraEU=lambda x: x.isIntraEU.map({"true":True,"false":False})
    )
    print("\n=== Summary per Destination ===")
    print(df_sum.head())

    # 2) Avg delay by Region
    q2 = """
    PREFIX ex: <http://example.org/flight/>
    SELECT ?region (AVG(xsd:double(?avgDelay)) AS ?meanDelay)
    WHERE {
      ?airport a ex:Airport ;
               ex:avgDelay ?avgDelay ;
               ex:hasRegion ?reg .
      BIND(STRAFTER(STR(?reg),"region/") AS ?region)
    }
    GROUP BY ?region
    ORDER BY DESC(?meanDelay)
    """
    df_reg = sparql_to_df(g, q2, ["region","mean_delay"])
    df_reg.mean_delay = df_reg.mean_delay.astype(float)
    print("\n=== Avg Delay by Region ===")
    print(df_reg)

    # 3) Avg delay by Volume Bucket
    q3 = """
    PREFIX ex: <http://example.org/flight/>
    SELECT ?bucket (AVG(xsd:double(?avgDelay)) AS ?meanDelay)
    WHERE {
      ?airport a ex:Airport ;
               ex:avgDelay ?avgDelay ;
               ex:inBucket ?b .
      BIND(STRAFTER(STR(?b),"volume/") AS ?bucket)
    }
    GROUP BY ?bucket
    ORDER BY ?bucket
    """
    df_buck = sparql_to_df(g, q3, ["bucket","mean_delay"])
    df_buck.mean_delay = df_buck.mean_delay.astype(float)
    print("\n=== Avg Delay by Volume Bucket ===")
    print(df_buck)

    # 4) Avg delay by DayOfWeek (no ORDER BY in SPARQL)
    q4 = """
    PREFIX ex: <http://example.org/flight/>
    SELECT ?dow (AVG(xsd:double(?delay)) AS ?meanDelay)
    WHERE {
      ?flight a ex:Flight ;
              ex:delayMinutes ?delay ;
              ex:dayOfWeek ?dow .
    }
    GROUP BY ?dow
    """
    df_dow = sparql_to_df(g, q4, ["dayOfWeek","mean_delay"])
    df_dow.mean_delay = df_dow.mean_delay.astype(float)

    # Reorder weekdays in natural order
    weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    df_dow["dayOfWeek"] = pd.Categorical(df_dow.dayOfWeek, categories=weekdays, ordered=True)
    df_dow = df_dow.sort_values("dayOfWeek").reset_index(drop=True)
    print("\n=== Avg Delay by Day of Week ===")
    print(df_dow)

    # 5) Correlations in Python
    pearson = df_sum.num_flights.corr(df_sum.avg_delay)
    spearman = df_sum.num_flights.corr(df_sum.avg_delay, method="spearman")
    print(f"\n=== H1 Correlations ===")
    print(f"Pearson r = {pearson:.3f}")
    print(f"Spearman ρ = {spearman:.3f}")

    # 6) Save CSVs
    os.makedirs("outputs", exist_ok=True)
    df_sum.to_csv("outputs/summary_per_destination.csv", index=False)
    df_reg.to_csv("outputs/avg_delay_by_region.csv", index=False)
    df_buck.to_csv("outputs/avg_delay_by_bucket.csv", index=False)
    df_dow.to_csv("outputs/avg_delay_by_weekday.csv", index=False)
    print("\n✅ Results saved to outputs/*.csv")

if __name__=="__main__":
    main()
