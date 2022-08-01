import json
import pickle
from pathlib import Path
from typing import Optional, Union
import logging

import treelib
from treelib import Tree
from treelib.exceptions import NodeIDAbsentError, DuplicatedNodeIdError


class Kind:
    """A class to represent an entity. It's used as argument for treelib node data"""

    def __init__(self, properties: set):
        """
        Args:
          properties: the state properties an entity has or could have in future
        """
        self.properties = properties


class Ontology:
    def __init__(
        self,
        ontology_file_path: Union[Path, str],
        ontology_data_model_path: Union[Path, str],
    ):
        self.ontology_file_path = Path(ontology_file_path)
        self.ontology_data_model_path = Path(ontology_data_model_path)

    def _load_ontology_graph(self) -> Optional[treelib.Tree]:
        """Loads ontology_graph.pickle and returns it as a tree object."""
        tree = None
        if self.ontology_file_path.exists():
            with open(self.ontology_file_path, "rb") as file:
                tree = pickle.load(file)
        return tree

    def _save_ontology_graph(self, tree: treelib.Tree):
        """Uploads tree to database/ontology_graph.pickle."""
        with open(self.ontology_file_path, "wb") as file:
            pickle.dump(tree, file)

    def _load_ontology_data_model(self) -> Optional[dict]:
        """Loads ontology data model json file and returns it as a dictionary."""
        data_model = None
        if self.ontology_data_model_path.exists():
            with open(self.ontology_data_model_path, "r", encoding='utf-8') as file:
                data_model = json.load(file)
        return data_model

    def _save_ontology_data_model(self, data_model: dict):
        """Dump a dictionary to ontology_data_model.json file."""
        with open(self.ontology_data_model_path, 'w', encoding='utf-8') as file:
            json.dump(data_model, file, indent=4)

    def create_kind(
        self,
        kind: str,
        parent: str = "Kind",
        start_tree: Optional[Tree] = None,
        kind_properties: Optional[set] = None,
    ) -> Tree:
        """Adds a given kind to the ontology_graph tree.

        Args:
          kind: kind to be added
          parent: parent of kind
          start_tree: treelib.Tree object, to which the new kind should be added
          kind_properties: A set of properties for the created kind

        Returns:
          tree object representing the ontology graph after the kind creation

        """
        if kind_properties is None:
            kind_properties = set()
        kind = kind.capitalize()
        parent = parent.capitalize()

        if start_tree is None:
            start_tree = self._load_ontology_graph()
            if start_tree is None:
                start_tree = Tree()
                start_tree.create_node(
                    tag="Kind",
                    identifier="Kind",
                    data=Kind(set(["_deleted"])),
                )
        tree = start_tree

        parent_node = tree.get_node(parent)
        if parent_node is None:
            tree = self.create_kind(parent, "Kind", tree)
            parent_node = tree.get_node(parent)
            logging.warning(
                "Not-in-database kind '%s'. Has been added as a child of 'Kind'", parent
            )

        kind_properties.update(parent_node.data.properties)  # type: ignore

        try:
            tree.create_node(
                tag=kind,
                identifier=kind,
                parent=parent,
                data=Kind(kind_properties),
            )
            self._save_ontology_graph(tree)
        except DuplicatedNodeIdError:
            logging.info(
                "The '%s' kind exists in database. No new kind has been created", kind
            )

        return tree

    def get_kind_node(self, tree: Optional[Tree], kind: str):
        """Searches tree for kind and returns the kind node

        Returns:
          kind node in case of success, None otherwise
        """
        if tree is None:
            logging.error("Ontology graph is empty")
            return None

        kind_node = tree.get_node(kind)
        if kind_node is None:
            logging.error("Kind '%s' is not in ontology graph", kind)
            return None
        return kind_node

    def remove_kind(self, kind: str):
        """Removes kind from database/ontology_graph"""
        tree = self._load_ontology_graph()
        if self.get_kind_node(tree, kind) is None:
            return None

        tree.remove_node(kind)

        self._save_ontology_graph(tree)
        logging.info(
            "Kind '%s' has been removed successfully from ontology graph", kind
        )

    def update_properties_of_kind(
        self, kind: str, old_properties: list, new_properties: list
    ):
        """Updates a list of properties of a given kind

        Returns:
          kind node in case of success, None otherwise
        """
        tree = self._load_ontology_graph()
        kind_node = self.get_kind_node(tree, kind)
        if kind_node is None:
            return None

        for idx, prop in enumerate(old_properties):
            if prop in kind_node.data.properties:
                kind_node.data.properties.remove(prop)
                kind_node.data.properties.add(new_properties[idx])
            else:
                logging.error("Property '%s' is not in '%s' properties", prop, kind)
                return None

        self._save_ontology_graph(tree)
        logging.info("Properties has been updated successfully")

    def create_properties_of_kind(self, kind: str, new_properties: list):
        """Creates a list of properties of a given kind

        Returns:
          kind node in case of success, None otherwise
        """
        tree = self._load_ontology_graph()
        kind_node = self.get_kind_node(tree, kind)
        if kind_node is None:
            return None

        for prop in new_properties:
            kind_node.data.properties.add(prop)

        self._save_ontology_graph(tree)
        logging.info("Properties has been updated successfully")

    def get_descendant_kinds(self, kind: str) -> Optional[list]:
        """Returns the children kinds of a given kind."""
        tree = self._load_ontology_graph()
        descendants = []
        if tree:
            try:
                descendants = [ch.tag for ch in tree.children(kind)]
            except NodeIDAbsentError:
                logging.error("Kind '%s' is not in ontology graph", kind)
                return None
        return descendants

    def get_kind_properties(self, kind: str) -> Optional[set]:
        """Returns the kind properties, stored in ontology graph"""
        tree = self._load_ontology_graph()
        kind_node = self.get_kind_node(tree, kind)
        if kind_node is not None:
            return kind_node.data.properties
        return set()

    def are_properties_in_kind(self, list_of_property_kinds, kind):
        """Checks if all the properties in the list are in fact properties of 'kind' in
        the ontology graph.
        """
        kind_properties = self.get_kind_properties(kind)
        for prop in list_of_property_kinds:
            if prop not in kind_properties:
                logging.error(
                    """The property '%s' isn't in '%s' properties in ontology graph.
                    Use create_properties_of_kind() function to add it""",
                    prop,
                    kind,
                )
                return False
        return True

    def create_relationship_model(
        self,
        kind_a: str,
        relationship_kind: str,
        kind_b: str,
        rel_properties: Optional[list] = None,
    ):
        """create a relationship kind between two entity kinds
        to make creation of such relationship in the graph possible.

        Args:
          kind_a: kind of first entity (from)
          relationship_kind: kind of relationship
          kind_b: kind of second entity (to)
          rel_properties: list of properties, a relationship could have

        Returns:

        """
        data_model = self._load_ontology_data_model()
        if rel_properties is None:
            rel_properties = []

        if data_model is None:
            data_model = dict()
        if relationship_kind in data_model:
            if (kind_a, kind_b) not in [
                (knd_a, knd_b) for (knd_a, knd_b, _) in data_model[relationship_kind]
            ]:
                data_model[relationship_kind].append([kind_a, kind_b, rel_properties])
            else:
                logging.info(
                    """Same relationship "(%s, %s, %s)" is already in the data model, """
                    """no new relationship kind was created""",
                    kind_a,
                    relationship_kind,
                    kind_b,
                )
                return None
        else:
            data_model.update({relationship_kind: [[kind_a, kind_b, rel_properties]]})
        self._save_ontology_data_model(data_model)
        logging.info(
            """Relationship "(%s, %s, %s)" was added to data model""",
            kind_a,
            relationship_kind,
            kind_b,
        )

    def get_relationship_kind_details(self, relationship_kind: str) -> Optional[list]:
        """Returns the relationship two-possible-parties as well as its properties."""
        data_model = self._load_ontology_data_model()
        if data_model is not None:
            kind = data_model.get(relationship_kind)
            return kind
        else:
            return None

    def is_valid_relationship(
        self, kind_a, relationship_kind, kind_b, rel_properties
    ) -> bool:
        """Checks if a relationship between two kinds is valid in the data model.

        Args:
          kind_a: kind of first entity (from)
          relationship_kind: kind of relationship
          kind_b: kind of second entity (to)
          rel_properties: list of properties, a relationship could have

        Returns:
          False in case the relationship is invalid (not in data model)
          True in case it is valid

        """
        data_model = self._load_ontology_data_model()
        if data_model is None:
            logging.error("The data model is empty")
            return False
        if relationship_kind not in data_model:
            logging.error(
                "Relationship kind '%s' is not in data model", relationship_kind
            )
            return False
        if (kind_a, kind_b) not in [
            (knd_a, knd_b) for (knd_a, knd_b, _) in data_model[relationship_kind]
        ]:
            logging.error(
                "The relationship kind '%s' is not supoorted between entities of kinds (%s, %s)",
                relationship_kind,
                kind_a,
                kind_b,
            )
            return False
        for (model_a, model_b, model_properties) in data_model[relationship_kind]:
            if (kind_a, kind_b) == (model_a, model_b):
                for prop in rel_properties:
                    if prop not in model_properties:
                        logging.error(
                            """The property '%s' isn't in '%s' properties in ontology data model.
                            Use create_properties_of_relationship_kind() function to add it""",
                            prop,
                            relationship_kind,
                        )
                        return False
        return True

    def show_data_model(
        self,
    ):
        """Displays the data model in a pretty way."""
        data_model = self._load_ontology_data_model()
        if data_model is None:
            logging.info("The data model is empty")
            return None
        for relationship_kind in data_model:
            for kind_a, kind_b, properties in data_model[relationship_kind]:
                print(f"({kind_a})-[{relationship_kind} {{{properties}}}]->({kind_b})")
