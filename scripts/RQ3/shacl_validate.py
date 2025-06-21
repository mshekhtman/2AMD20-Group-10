from pyshacl import validate
from rdflib import Graph
from pathlib import Path

# Paths
data_ttl   = "data/new_rq3/rq3_knowledge_graph.ttl"
shapes_ttl = "ontology/shapes.ttl"
report     = Path("data/new_rq3/shacl_report.txt")

# Load data graph
data_graph = Graph().parse(data_ttl, format="turtle")

# Validate
conforms, v_graph, v_text = validate(
    data_graph=data_graph,
    shacl_graph=shapes_ttl,
    inference='rdfs',
    abort_on_first=False,
    meta_shacl=False,
    advanced=True,
    debug=False
)

# Write results
with report.open("w") as f:
    f.write("Conforms: " + str(conforms) + "\n\n")
    f.write(v_text)

print(f"SHACL validation complete. Conforms={conforms}")
print(f"Report written to {report}")