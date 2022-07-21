from typing import Optional
import os
import pickle
import treelib


def load_ontology_graph() -> Optional[treelib.Tree]:
    """Loads ontology_graph.pickle and returns it as a tree object."""
    tree = None
    ontology_graph_db = "deeppavlov_kg/database/ontology_graph.pickle"
    if os.path.exists(ontology_graph_db):
        with open(ontology_graph_db, "rb") as file:
            tree = pickle.load(file)
    return tree


def is_identical_id(id_: str) -> bool:
    """Checks if the given id is in the database or not."""
    ids = []
    ids_file = "deeppavlov_kg/database/db_ids.txt"
    if os.path.exists(ids_file):
        with open(ids_file, "r+", encoding="utf-8") as file:
            for line in file:
                ids.append(line.strip())
    else:
        open(ids_file, "w", encoding="utf-8").close()
    if id_ in ids:
        return False
    else:
        return True


def store_id(id_):
    """Saves the given id to a db_ids file."""
    with open("deeppavlov_kg/database/db_ids.txt", "a", encoding="utf-8") as file:
        file.write(id_ +"\n")
