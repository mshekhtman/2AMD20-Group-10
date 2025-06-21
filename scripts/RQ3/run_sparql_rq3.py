from rdflib import Graph
from pathlib import Path

g = Graph()
g.parse(Path.cwd() / "ontology/schema.ttl", format="turtle")
g.parse(Path.cwd() / "data/new_rq3/rq3_knowledge_graph.ttl", format="turtle")

query_text = (Path.cwd() / "queries/rq3_query.sparql").read_text()
results = g.query(query_text)

print("Above-average airports (IATA, Passengers, Delay):")
for row in results:
    iata       = row.iata.toPython()
    passengers = row.passengers.toPython()
    delay      = row.delay.toPython()
    print(f"{iata}: passengers={passengers}, delay={delay:.2f} min")
