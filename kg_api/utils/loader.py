from typing import Optional
from os.path import exists
import pickle
import treelib


def load_ontology_graph() -> Optional[treelib.Tree]:
    """Loads ontology_graph.pickle and returns it as a tree object."""
    tree = None
    ontology_graph_db = "kg_api/database/ontology_graph.pickle"
    if exists(ontology_graph_db):
        with open(ontology_graph_db, "rb") as file:
            tree = pickle.load(file)
    return tree


