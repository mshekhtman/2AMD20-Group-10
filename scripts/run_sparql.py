#!/usr/bin/env python3
import os
import json
import pandas as pd
from rdflib import Graph, Namespace, Literal, RDF
from rdflib.namespace import XSD
from pyshacl import validate

EX = Namespace("http://example.org/flight/")
SCHEMA = "ontology/schema.ttl"
SHAPES = "ontology/shapes.ttl"

def build_kg():
    g = Graph()
    g.bind("ex", EX)

   
