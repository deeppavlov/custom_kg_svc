import json
from typing import Optional
import os
import pickle
import treelib
from deeppavlov_kg.utils.settings import OntologySettings

ontology_settings = OntologySettings()


def load_ontology_graph() -> Optional[treelib.Tree]:
    """Loads ontology_graph.pickle and returns it as a tree object."""
    tree = None
    ontology_graph_db = ontology_settings.ontology_file_path
    if os.path.exists(ontology_graph_db):
        with open(ontology_graph_db, "rb") as file:
            tree = pickle.load(file)
    return tree


def save_ontology_graph(tree: treelib.Tree):
    """Uploads tree to database/ontology_graph.pickle."""
    with open(ontology_settings.ontology_file_path, "wb") as file:
        pickle.dump(tree, file)


def is_identical_id(id_: str) -> bool:
    """Checks if the given id is in the database or not."""
    ids = []
    ids_file = ontology_settings.db_ids_file_path
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
    with open(ontology_settings.db_ids_file_path, "a", encoding="utf-8") as file:
        file.write(id_ +"\n")


def load_ontology_data_model() -> Optional[dict]:
    """Loads ontology data model json file and returns it as a dictionary."""
    data_model = None
    data_model_path = ontology_settings.ontology_data_model_path
    if os.path.exists(data_model_path):
        with open(data_model_path, "r", encoding='utf-8') as file:
            data_model = json.load(file)
    return data_model


def save_ontology_data_model(data_model: dict):
    """Dump a dictionary to ontology_data_model.json file."""
    with open(ontology_settings.ontology_data_model_path, 'w', encoding='utf-8') as file:
        json.dump(data_model, file, indent=4)
