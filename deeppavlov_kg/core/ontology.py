import datetime
import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Set, Type, Optional, Union
import logging

import treelib
from treelib import Tree
from treelib.exceptions import NodeIDAbsentError, DuplicatedNodeIdError


class Kind:
    """A class to represent an entity. It's used as argument for treelib node data"""

    def __init__(self, properties: Dict[str, Any]):
        """
        Args:
          properties: the state properties an entity has or could have in future
        """
        self.properties = properties


class Ontology:
    def __init__(
        self,
        ontology_kinds_hierarchy_path: Union[Path, str],
        ontology_data_model_path: Union[Path, str],
    ):
        self.ontology_kinds_hierarchy_path = Path(ontology_kinds_hierarchy_path)
        self.ontology_data_model_path = Path(ontology_data_model_path)

    def _load_ontology_kinds_hierarchy(self) -> Optional[treelib.Tree]:
        """Loads ontology_kinds_hierarchy.pickle and returns it as a tree object."""
        tree = None
        if self.ontology_kinds_hierarchy_path.exists():
            with open(self.ontology_kinds_hierarchy_path, "rb") as file:
                tree = pickle.load(file)
        return tree

    def _save_ontology_kinds_hierarchy(self, tree: treelib.Tree):
        """Uploads tree to database/ontology_kinds_hierarchy.pickle."""
        with open(self.ontology_kinds_hierarchy_path, "wb") as file:
            pickle.dump(tree, file)

    def _load_ontology_data_model(self) -> Optional[Dict[str, list]]:
        """Loads ontology data model json file and returns it as a dictionary."""
        data_model = None
        if self.ontology_data_model_path.exists():
            with open(self.ontology_data_model_path, "r", encoding="utf-8") as file:
                data_model = json.load(file)
        return data_model

    def _save_ontology_data_model(self, data_model: dict):
        """Dump a dictionary to ontology_data_model.json file."""
        with open(self.ontology_data_model_path, "w", encoding="utf-8") as file:
            json.dump(data_model, file, indent=4)

    def _type2str(self, types_to_convert: List[Type]) -> List[str]:
        """Converts list of types to a list of strings."""
        types_str = []
        types = {
            str: str(str),
            int: str(int),
            float: str(float),
            bool: str(bool),
            datetime.date: str(datetime.date),
            datetime.time: str(datetime.time),
            datetime.datetime: str(datetime.datetime),
        }
        for item in types_to_convert:
            types_str.append(types.get(item))
        return types_str

    def _get_node_from_tree(self, tree: Optional[Tree], kind: str):
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

    def create_entity_kind(
        self,
        kind: str,
        parent: str = "Kind",
        start_tree: Optional[Tree] = None,
        kind_properties: Optional[Set[str]] = None,
        kind_property_types: Optional[List[Type]] = None,
        kind_property_measurement_units: Optional[List[str]] = None,
    ) -> Tree:
        """Adds a given kind to the ontology_kinds_hierarchy tree.

        Args:
          kind: kind to be added
          parent: parent of kind
          start_tree: treelib.Tree object, to which the new kind should be added
          kind_properties: A set of properties for the created kind
          kind_property_types: A list of property types that correspond to items in
                               kind_properties respectively respectively by index
          kind_property_measurement_units: A list of measurement units that correspond to items in
                               kind_properties respectively by index

        Returns:
          tree object representing the ontology graph after the kind creation

        """
        if kind_properties is None:
            kind_properties = set()
        if kind_property_types is None:
            kind_property_types = [str] * (len(kind_properties))
        if kind_property_measurement_units is None:
            kind_property_measurement_units = [""] * (len(kind_properties))
        
        if len(kind_property_types) != len(kind_property_measurement_units):
            logging.error("Number of property types doesn't correspond properly with number of"
                          " property measurement_units. They should be equal")

        kind = kind.capitalize()
        parent = parent.capitalize()

        if start_tree is None:
            start_tree = self._load_ontology_kinds_hierarchy()
            if start_tree is None:
                start_tree = Tree()
                start_tree.create_node(
                    tag="Kind",
                    identifier="Kind",
                    data=Kind({
                        "_deleted": {"type": str(bool), "measurement_unit": ""},
                    }),
                )
        tree = start_tree

        parent_node = tree.get_node(parent)
        if parent_node is None:
            tree = self.create_entity_kind(parent, "Kind", tree)
            parent_node = tree.get_node(parent)
            logging.warning(
                "Not-in-database kind '%s'. Has been added as a child of 'Kind'", parent
            )
        kind_properties_dict = {}
        for idx, prop in enumerate(kind_properties):
            kind_properties_dict.update(
                {prop: {"type": self._type2str(kind_property_types)[idx]}}
            )
        for idx, prop in enumerate(kind_properties):
            kind_properties_dict[prop].update(
                {"measurement_unit": kind_property_measurement_units[idx]}
            )
        kind_properties_dict.update(parent_node.data.properties)  # type: ignore

        try:
            tree.create_node(
                tag=kind,
                identifier=kind,
                parent=parent,
                data=Kind(kind_properties_dict),
            )
            self._save_ontology_kinds_hierarchy(tree)
        except DuplicatedNodeIdError:
            logging.info(
                "The '%s' kind exists in database. No new kind has been created", kind
            )

        return tree

    def remove_entity_kind(self, kind: str):
        """Removes kind from database/ontology_kinds_hierarchy"""
        tree = self._load_ontology_kinds_hierarchy()
        if self._get_node_from_tree(tree, kind) is None:
            return None

        tree.remove_node(kind)

        self._save_ontology_kinds_hierarchy(tree)
        logging.info(
            "Kind '%s' has been removed successfully from ontology graph", kind
        )

    def update_entity_kind_properties(
        self,
        kind: str,
        old_property_kinds: List[str],
        new_property_kinds: Optional[List[str]] = None,
        new_property_types: Optional[List[Type]] = None,
        new_property_measurement_units: Optional[List[str]] = None,
    ):
        """Updates a list of properties of a given kind

        Args:
          kind: Entity kind
          old_property_kinds: Properties to be updated
          new_property_kinds: New property kinds (If they were to be changed)
          new_property_types: A list of property types that correspond to items in
                               kind_properties respectively by index
          new_property_measurement_unit: A list of measurement units that correspond to items in
                               kind_properties respectively by index

        Returns:
          kind node in case of success, None otherwise

        """
        if new_property_types is None:
            new_property_types = [str] * len(old_property_kinds)
        if new_property_measurement_units is None:
            new_property_measurement_units = [""] * len(old_property_kinds)
        for lst in [
            new_property_kinds,
            new_property_types,
            new_property_measurement_units,
        ]:
            if lst is not None and len(lst) != len(old_property_kinds):
                logging.error(
                    "Number of old property kinds doesn't correspond properly with number of"
                    "new property kinds, types or measurement_units. All should be equal"
                )
                return None
        tree = self._load_ontology_kinds_hierarchy()
        if tree is None:
            logging.error(
                "Ontology kinds hierarchy is empty. Couldn't update entity kind properties"
            )
            return None
        kind_node = self._get_node_from_tree(tree, kind)
        if kind_node is None:
            return None

        for idx, prop in enumerate(old_property_kinds):
            if prop in kind_node.data.properties:
                prop_details = kind_node.data.properties[prop]
                del kind_node.data.properties[prop]
                prop_details["type"] = self._type2str(new_property_types)[idx]
                prop_details["measurement_unit"] = new_property_measurement_units[
                    idx
                ]
                properties_kinds = (
                    old_property_kinds
                    if new_property_kinds is None
                    else new_property_kinds
                )
                kind_node.data.properties[properties_kinds[idx]] = prop_details
            else:
                logging.error("Property '%s' is not in '%s' properties", prop, kind)
                return None

        self._save_ontology_kinds_hierarchy(tree)
        logging.info("Properties has been updated successfully")
        return kind_node

    def create_entity_kind_properties(
        self,
        kind: str,
        new_property_kinds: List[str],
        new_property_types: Optional[List[Type]] = None,
        new_property_measurement_units: Optional[List[str]] = None,
    ):
        """Creates a list of properties of a given kind

        Args:
          new_property_kinds: New property kinds
          new_property_types: A list of property types that correspond to items in
                               kind_properties respectively by index
          new_property_measurement_unit: A list of measurement units that correspond to items in
                               kind_properties respectively by index

        Returns:
          kind node in case of success, None otherwise
        """
        if new_property_types is None:
            new_property_types = [str] * len(new_property_kinds)
        if new_property_measurement_units is None:
            new_property_measurement_units = [""] * len(new_property_kinds)

        if not (
            len(new_property_kinds)
            == len(new_property_types)
            == len(new_property_measurement_units)
        ):
            logging.error(
                "Number of new property kinds doesn't correspond properly with number of "
                "new property types or measurement_units. All should be equal"
            )
            return None

        tree = self._load_ontology_kinds_hierarchy()
        if tree is None:
            logging.error(
                "Ontology kinds hierarchy is empty. Couldn't create entity kind properties"
            )
            return None
        kind_node = self._get_node_from_tree(tree, kind)
        if kind_node is None:
            return None

        for idx, prop in enumerate(new_property_kinds):
            kind_node.data.properties.update(
                {
                    prop: {
                        "type": self._type2str(new_property_types)[idx],
                        "measurement_unit": new_property_measurement_units[idx],
                    }
                }
            )

        self._save_ontology_kinds_hierarchy(tree)
        logging.info("Properties has been updated successfully")
        return kind_node

    def get_descendants_of_entity_kind(self, kind: str) -> Optional[List[str]]:
        """Returns the children kinds of a given kind."""
        tree = self._load_ontology_kinds_hierarchy()
        descendants = []
        if tree:
            try:
                descendants = [ch.tag for ch in tree.children(kind)]
            except NodeIDAbsentError:
                logging.error("Kind '%s' is not in ontology graph", kind)
                return None
        return descendants

    def get_entity_kind_properties(self, kind: str) -> Dict[str, dict]:
        """Returns the kind properties, stored in ontology graph"""
        tree = self._load_ontology_kinds_hierarchy()
        kind_node = self._get_node_from_tree(tree, kind)
        if kind_node is not None:
            return kind_node.data.properties
        return dict()

    def is_valid_entity_kind(self, kind: str) -> bool:
        """Checks if a given kind exists in ontology kinds hierarchy.

        Returns:
          True in case it exists
          False otherwise
        """
        tree = self._load_ontology_kinds_hierarchy()
        if tree is not None and tree.get_node(kind) is not None:
            return True
        return False

    def are_valid_entity_kind_properties(
        self,
        list_of_property_kinds: List[str],
        list_of_property_values: List[Any],
        entity_kind: str,
    ):
        """Checks if all the properties in the list are in fact properties of 'kind' in
        the ontology graph.
        """
        kind_properties = self.get_entity_kind_properties(entity_kind)
        for idx, prop in enumerate(list_of_property_kinds):
            if prop not in kind_properties:
                logging.error(
                    """The property '%s' isn't in '%s' properties in ontology graph.
                    Use create_properties_of_kind() function to add it""",
                    prop,
                    entity_kind,
                )
                return False

            property_type = kind_properties[prop]["type"]
            if str(type(list_of_property_values[idx])) != property_type:
                logging.error(
                    "Property '%s' should be of type: '%s'", prop, property_type
                )
                return False
        return True

    def show_entity_kinds_hierarchy(self, with_properties: bool = False):
        """Displays the ontology kinds hierarchy in form of tree.

        Args:
          with_properties: False to show kinds. True to show kind properties in the hierarchy.
        """
        tree = self._load_ontology_kinds_hierarchy()
        if tree is None:
            logging.error(
                "Ontology kinds hierarchy is empty. Can't show entity kinds hierarchy"
            )
            return None
        if with_properties:
            tree.show(data_property="properties")
        else:
            tree.show()

    def create_relationship_kind(
        self,
        relationship_kind: str,
        kind_a: str = "All",
        kind_b: str = "All",
        rel_property_kinds: Optional[List[str]] = None,
        rel_property_types: Optional[List[Type]] = None,
        rel_property_measurement_units: Optional[List[str]] = None,
    ):
        """create a relationship kind between two entity kinds
        to make creation of such relationship in the graph possible.

        Args:
          kind_a: kind of first entity (from)
          relationship_kind: kind of relationship
          kind_b: kind of second entity (to)
          rel_property_kinds: list of properties, a relationship could have,
          kind_property_types: A list of property types that correspond to items in
                               rel_property_kinds respectively by index
          kind_property_measurement_units: A list of measurement units that correspond to items in
                               rel_property_kinds respectively by index

        Returns:

        """
        if rel_property_kinds is None:
            rel_property_kinds = []
        if rel_property_types is None:
            rel_property_types = [str] * (len(rel_property_kinds))
        if rel_property_measurement_units is None:
            rel_property_measurement_units = [""] * (len(rel_property_kinds))

        rel_properties_dict = {}
        for idx, prop in enumerate(rel_property_kinds):
            rel_properties_dict.update(
                {
                    prop: {
                        "type": self._type2str(rel_property_types)[idx],
                        "measurement_unit": rel_property_measurement_units[idx],
                    },
                }
            )
        rel_properties_dict.update({
            "_deleted": {"type": str(bool), "measurement_unit": ""},
        })

        data_model = self._load_ontology_data_model()
        if data_model is None:
            data_model = dict()
        if relationship_kind in data_model:
            if (kind_a, kind_b) not in [
                (knd_a, knd_b) for (knd_a, knd_b, _) in data_model[relationship_kind]
            ]:
                data_model[relationship_kind].append(
                    [kind_a, kind_b, rel_properties_dict]
                )
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
            data_model.update(
                {relationship_kind: [[kind_a, kind_b, rel_properties_dict]]}
            )
        self._save_ontology_data_model(data_model)
        logging.info(
            """Relationship "(%s, %s, %s)" was added to data model""",
            kind_a,
            relationship_kind,
            kind_b,
        )

    def create_relationship_kind_properties(
        self,
        kind_a: str,
        relationship_kind: str,
        kind_b: str,
        new_property_kinds: List[str],
        new_property_types: Optional[List[Type]] = None,
        new_property_measurement_units: Optional[List[str]] = None,
    ):
        """Creates a list of properties of a given kind

        Args:
          new_properties: list of properties, a relationship could have,
          new_property_types: A list of property types that correspond to items in
                              new_properties respectively by index
          new_property_measurement_units: A list of measurement units that correspond to items in
                              new_properties respectively by index

        Returns:
          data model in case of success, None otherwise
        """
        if new_property_types is None:
            new_property_types = [str] * len(new_property_kinds)
        if new_property_measurement_units is None:
            new_property_measurement_units = [""] * len(new_property_kinds)

        if not (
            len(new_property_kinds)
            == len(new_property_types)
            == len(new_property_measurement_units)
        ):
            logging.error(
                "Number of new properties kinds doesn't correspond properly with number of "
                "new property kinds or values. All should be equal"
            )
            return None

        data_model = self._load_ontology_data_model()
        if data_model is None:
            logging.error("Data model is empty. Couldn't find relationships")
            return None
        if relationship_kind in data_model:
            for idx, (knd_a, knd_b, _) in enumerate(data_model[relationship_kind]):
                if (kind_a, kind_b) == (knd_a, knd_b):
                    for prop in new_property_kinds:
                        data_model[relationship_kind][idx][2][prop] = {
                            "type": self._type2str(new_property_types)[idx],
                            "measurement_unit": new_property_measurement_units[idx],
                        }
            self._save_ontology_data_model(data_model)
        else:
            logging.error(
                "Relationship_kind '%s' is not in data model", relationship_kind
            )
        return data_model


    def get_relationship_kind_details(
        self, relationship_kind: str
    ) -> Optional[List[List]]:
        """Returns the relationship two-possible-parties as well as its properties."""
        data_model = self._load_ontology_data_model()
        if data_model is not None:
            kind = data_model.get(relationship_kind)
            return kind
        else:
            return None

    def is_valid_relationship_model(
        self,
        kind_a: str,
        relationship_kind: str,
        kind_b: str,
        rel_property_kinds: List[str],
        rel_property_values: List[Any],
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
        for (knd_a, knd_b, _) in data_model[relationship_kind]:
            if (
                (knd_a == kind_a and knd_b == kind_b)
                or (knd_a == "All" and knd_b == kind_b)
                or (knd_a == kind_a and knd_b == "All")
                or (knd_a == "All" and knd_b == "All")
            ):
                continue
            else:
                logging.error(
                    "The relationship kind '%s' is not supoorted between entities of kinds (%s, %s)",
                    relationship_kind,
                    kind_a,
                    kind_b,
                )
                return False

        for (model_a, model_b, model_properties) in data_model[relationship_kind]:
            if (kind_a, kind_b) == (model_a, model_b):
                for idx, prop in enumerate(rel_property_kinds):
                    if prop not in model_properties:
                        logging.error(
                            """The property '%s' isn't in '%s' properties in ontology data model.
                            Use create_properties_of_relationship_kind() function to add it""",
                            prop,
                            relationship_kind,
                        )
                        return False
                    else:
                        property_type = model_properties[prop]["type"]
                        if str(type(rel_property_values[idx])) != property_type:
                            logging.error(
                                "Property '%s' should be of type: '%s'",
                                prop,
                                property_type,
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
                print(f"({kind_a})-[{relationship_kind} [{properties}]]->({kind_b})")
