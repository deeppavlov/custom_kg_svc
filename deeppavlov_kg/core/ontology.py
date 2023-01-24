import datetime
import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Type, Optional, Union, Tuple
import logging

import treelib
from treelib import Tree
from treelib.exceptions import NodeIDAbsentError, DuplicatedNodeIdError
import re
from terminusdb_client.errors import DatabaseError
from terminusdb_client import WOQLQuery as WOQL

class Kind:
    """A class to represent an entity. It's used as argument for treelib node data"""

    def __init__(self, properties: Dict[str, Any]):
        """
        Args:
        properties: the state properties an entity has or could have in future
        """
        self.properties = properties


class OntologyConfig:
    def __init__(self,):
        raise NotImplementedError

    def create_entity_kinds(self, entity_kinds: List[str], parents: Optional[List[Union[str, None]]] = None):
        raise NotImplementedError

    def create_entity_kind(self, entity_kind: str, parent: Optional[str] = None):
        raise NotImplementedError

    def delete_entity_kind(self, entity_kind: str):
        raise NotImplementedError

    def get_all_entity_kinds(self):
        raise NotImplementedError

    def get_entity_kind(self, entity_kind: str):
        raise NotImplementedError

    def create_property_kinds_of_entity_kinds(
            self,
            entity_kinds: List[str],
            property_kinds: List[List[str]],
            property_types: Optional[List[List[Type]]]= None
        ):
        raise NotImplementedError

    def create_property_kinds_of_entity_kind(self, entity_kinds: List[str], property_kinds: List[str], property_types: Optional[List[Type]]= None):
        raise NotImplementedError

    def create_property_kind_of_entity_kind(self, entity_kind: str, property_kind: str, property_type: Type):
        raise NotImplementedError

    def delete_property_kinds(self, entity_kind: str, property_kinds: List[str]):
        raise NotImplementedError

    def delete_property_kind(self, entity_kind: str, property_kind: str):
        raise NotImplementedError

    def create_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str], entity_kinds_b: List[str]):
        raise NotImplementedError

    def create_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        raise NotImplementedError

    def get_relationship_kind(self, relationship_kind: str):
        raise NotImplementedError

    def delete_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str], entity_kinds_b: List[str]):
        raise NotImplementedError

    def delete_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        raise NotImplementedError


class Neo4jOntologyConfig(OntologyConfig):

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

    def _type2str(self, types_to_convert: List[Type]) -> List[str]: # TODO: make it staticmethod
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

    def _node2dict(self, node: treelib.node.Node) -> dict:
        return {"kind": node.tag, **node.data.properties}

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

    def _check_entity_kind_properties_validity(
        self,
        list_of_property_kinds: List[str],
        list_of_property_values: List[Any],
        entity_kind: str,
    ):
        """Checks for presence of the given property kinds in the ontology and checks if the
        property value type matches the expected type in ontology.
        """
        kind_properties = self.get_entity_kind(entity_kind)
        for idx, prop in enumerate(list_of_property_kinds):
            if prop not in kind_properties:
                raise ValueError(
                    """The property '%s' isn't in '%s' properties in ontology graph.
                    Use create_properties_of_kind() function to add it""",
                    prop,
                    entity_kind,
                )

            property_type = kind_properties[prop]["type"]
            if str(type(list_of_property_values[idx])) != property_type:
                raise ValueError(
                    "Property '%s' should be of type: '%s'", prop, property_type
                )
        return True

    def _is_valid_relationship_model(
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
          rel_property_kinds: list of property kinds
          rel_property_values: list of property values

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

        if [kind_a, kind_b] not in [rel[:2] for rel in data_model[relationship_kind]]:
            raise ValueError(f"The relationship kind '{relationship_kind}' is not supoorted between entities of kinds ({kind_a}, {kind_b})")

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

    def create_entity_kinds(self):
        raise NotImplementedError

    def create_entity_kind(
        self,
        kind: str,
        parent: str = "Kind",
        kind_properties: Optional[List[str]] = None,
        kind_property_types: Optional[List[Type]] = None,
        kind_property_measurement_units: Optional[List[str]] = None,
    ) -> dict:
        """Adds a given kind to the ontology_kinds_hierarchy tree.

        Args:
          kind: kind to be added
          parent: parent of kind
          kind_properties: A set of properties for the created kind
          kind_property_types: A list of property types that correspond to items in
                               kind_properties respectively respectively by index
          kind_property_measurement_units: A list of measurement units that correspond to items in
                               kind_properties respectively by index

        Returns:
          entity kind properties

        """
        if kind_properties is None:
            kind_properties = []
        if kind_property_types is None:
            kind_property_types = [str] * (len(kind_properties))
        if kind_property_measurement_units is None:
            kind_property_measurement_units = [""] * (len(kind_properties))

        assert len(kind_property_types) == len(kind_property_measurement_units), (
                "Number of property types doesn't correspond properly with number of"
                " property measurement_units. They should be equal"
            )

        # kind = kind.capitalize()
        # parent = parent.capitalize()

        tree = self._load_ontology_kinds_hierarchy()
        if tree is None:
            tree = Tree()
            tree.create_node(
                tag="Kind",
                identifier="Kind",
                data=Kind(
                    {
                        "_deleted": {"type": str(bool), "measurement_unit": ""},
                    }
                ),
            )

        parent_node = tree.get_node(parent)
        if parent_node is None:
            parent_node = self.create_entity_kind(parent, "Kind")
            tree = self._load_ontology_kinds_hierarchy()
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

        kind_node = self._node2dict(tree.get_node(kind))
        
        return kind_node

    def delete_entity_kind(self, entity_kind: str):
        """Removes kind from database/ontology_kinds_hierarchy"""
        tree = self._load_ontology_kinds_hierarchy()
        if tree is None or self._get_node_from_tree(tree, entity_kind) is None:
            return None

        tree.remove_node(entity_kind)

        self._save_ontology_kinds_hierarchy(tree)
        logging.info(
            "Kind '%s' has been removed successfully from ontology graph", entity_kind
        )

    def get_all_entity_kinds(self):
        raise NotImplementedError

    def get_entity_kind(self, entity_kind: str):
        """Returns the kind properties, stored in ontology graph"""
        tree = self._load_ontology_kinds_hierarchy()
        kind_node = self._get_node_from_tree(tree, entity_kind)
        if kind_node is not None:
            return kind_node.data.properties
        return dict()

    def create_property_kinds_of_entity_kinds(
            self,
            entity_kinds: List[str],
            property_kinds: List[List[str]],
            property_types: Optional[List[List[Type]]]= None
        ):
        raise NotImplementedError

    def create_property_kinds_of_entity_kind(
        self,
        kind: str,
        property_kinds: List[str],
        property_types: Optional[List[Type]] = None,
        # new_property_measurement_units: Optional[List[str]] = None,
    ) -> dict:
        """Creates a list of properties of a given kind

        Args:
          kind: entity kind to which we're creating properties
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
        # if new_property_measurement_units is None:
        #     new_property_measurement_units = [""] * len(new_property_kinds)

        assert (
            len(new_property_kinds)
            == len(new_property_types)
            # == len(new_property_measurement_units)
        ), (
                "Number of new property kinds doesn't correspond properly with number of "
                "new property types or measurement_units. All should be equal"
            )

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
                        # "measurement_unit": new_property_measurement_units[idx],
                    }
                }
            )

        self._save_ontology_kinds_hierarchy(tree)
        logging.info("Properties has been updated successfully")
        return self._node2dict(kind_node)

    def create_property_kind_of_entity_kind(self, entity_kind: str, property_kind: str, property_type: Type):
        self.create_property_kinds_of_entity_kind(entity_kind, [property_kind], [property_type])

    def delete_property_kinds(self, entity_kind: str, property_kinds: List[str]):
        """Deletes property kinds that relate to specific entity kind from the ontology"""
        tree = self._load_ontology_kinds_hierarchy()
        kind_node = self._get_node_from_tree(tree, entity_kind)
        if kind_node is not None and tree is not None:
            for property_kind in property_kinds:
                if property_kind in kind_node.data.properties:
                    kind_node.data.properties.pop(property_kind)
                else:
                    raise ValueError(f"The property:'{property_kind}' does not exist in ontology kinds hierarchy. "
                                      "no property was deleted.")

            self._save_ontology_kinds_hierarchy(tree)
            logging.info("Property kinds has been deleted successfully")
        else:
            raise ValueError(f"The ontology kinds hierarych is empty or the entity kind '{entity_kind}' doesn't exist. "
                              "no property was deleted.")

    def delete_property_kind(self, entity_kind: str, property_kind: str):
        """Deletes property kind that relates to specific entity kind from the ontology"""
        return self.delete_property_kinds(entity_kind, [property_kind])

    def create_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str], entity_kinds_b: List[str]):
        raise NotImplementedError

    def create_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        """create a relationship kind between two entity kinds
        to make creation of such relationship in the graph possible.

        Args:
          entity_kind_a: kind of first entity (from)
          relationship_kind: kind of relationship
          entity_kind_b: kind of second entity (to)
          rel_property_kinds: list of properties, a relationship could have,
          kind_property_types: A list of property types that correspond to items in
                               rel_property_kinds respectively by index
          kind_property_measurement_units: A list of measurement units that correspond to items in
                               rel_property_kinds respectively by index

        Returns:

        """
        # if rel_property_kinds is None:
        #     rel_property_kinds = []
        # if rel_property_types is None:
        #     rel_property_types = [str] * (len(rel_property_kinds))
        # if rel_property_measurement_units is None:
        #     rel_property_measurement_units = [""] * (len(rel_property_kinds))

        rel_properties_dict = {}
        # for idx, prop in enumerate(rel_property_kinds):
        #     rel_properties_dict.update(
        #         {
        #             prop: {
        #                 "type": self._type2str(rel_property_types)[idx],
        #                 "measurement_unit": rel_property_measurement_units[idx],
        #             },
        #         }
        #     )
        # rel_properties_dict.update(
        #     {
        #         "_deleted": {"type": str(bool), "measurement_unit": ""},
        #     }
        # )

        data_model = self._load_ontology_data_model()
        if data_model is None:
            data_model = dict()
        if relationship_kind in data_model:
            if (entity_kind_a, entity_kind_b) not in [
                (knd_a, knd_b) for (knd_a, knd_b, _) in data_model[relationship_kind]
            ]:
                data_model[relationship_kind].append(
                    [entity_kind_a, entity_kind_b, rel_properties_dict]
                )
            else:
                logging.info(
                    """Same relationship "(%s, %s, %s)" is already in the data model, """
                    """no new relationship kind was created""",
                    entity_kind_a,
                    relationship_kind,
                    entity_kind_b,
                )
                return None
        else:
            data_model.update(
                {relationship_kind: [[entity_kind_a, entity_kind_b, rel_properties_dict]]}
            )
        self._save_ontology_data_model(data_model)
        logging.info(
            """Relationship "(%s, %s, %s)" was added to data model""",
            entity_kind_a,
            relationship_kind,
            entity_kind_b,
        )

    def get_relationship_kind(self, relationship_kind: str):
        """Returns the relationship two-possible-parties as well as its properties."""
        data_model = self._load_ontology_data_model()
        if data_model is not None:
            kind = data_model.get(relationship_kind)
            return kind
        else:
            return None

    def delete_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str], entity_kinds_b: List[str]):
        raise NotImplementedError

    def delete_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        data_model = self._load_ontology_data_model()
        if data_model is not None:
            if relationship_kind in data_model:
                for idx, relationship in enumerate(data_model[relationship_kind]):
                    if [entity_kind_a, entity_kind_b] == [relationship[0], relationship[1]]:
                        data_model[relationship_kind].pop(idx)
                self._save_ontology_data_model(data_model)
                logging.info("relationship kinds has been deleted successfully")
            else:
                raise ValueError("The given relationship doesn't exist. No relationship kind has been deleted")
        else:
            raise ValueError("The data model is empty. No relationship kind has been deleted")

    # Extra
    def create_relationship_property_kinds(
        self,
        relationship_kind: str,
        new_property_kinds: List[str],
        new_property_types: Optional[List[Type]] = None,
        # new_property_measurement_units: Optional[List[str]] = None,
    ):
        """Creates a list of properties for a relationship

        Args:
          relationship_kind: kind of relationship
          new_property_kinds: list of properties, a relationship could have,
          new_property_types: A list of property types that correspond to items in
                               rel_property_kinds respectively by index
          kind_property_measurement_units: A list of measurement units that correspond to items in
                               rel_property_kinds respectively by index

        Returns:
          data model in case of success, None otherwise
        """
        if new_property_types is None:
            new_property_types = [str] * len(new_property_kinds)
        # if new_property_measurement_units is None:
        #     new_property_measurement_units = [""] * len(new_property_kinds)

        assert (
            len(new_property_kinds)
            == len(new_property_types)
            # == len(new_property_measurement_units)
        ), (
                "Number of new properties kinds doesn't correspond properly with number of "
                "new property kinds or values. All should be equal"
            )

        data_model = self._load_ontology_data_model()
        if data_model is None:
            logging.error("Data model is empty. Couldn't find relationships")
            return None
        if relationship_kind in data_model:
            for model_idx, (knd_a, knd_b, _) in enumerate(data_model[relationship_kind]):
                if (kind_a, kind_b) == (knd_a, knd_b):
                    for prop_idx, prop in enumerate(new_property_kinds):
                        data_model[relationship_kind][model_idx][2][prop] = {
                            "type": self._type2str(new_property_types)[prop_idx],
                            # "measurement_unit": new_property_measurement_units[prop_idx],
                        }
            self._save_ontology_data_model(data_model)
        else:
            raise ValueError(
                "Relationship_kind '%s' is not in data model", relationship_kind
            )
        return data_model[relationship_kind]

    # Extra
    def update_relationship_property_kinds(self):
        raise NotImplementedError

    # Extra
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

    # Extra
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


# no need for update function, just use delete and create, because anyway you can't update/delete
# while there're docs in the database
class TerminusdbOntologyConfig(OntologyConfig):
    def __init__(
        self,
        client,
        kg,
    ):
        self._client = client
        self.kg = kg
        if self._client.get_class_frame("Abstract").get("Name") is None:
            self.init_abstract_kind()

    def init_abstract_kind(self):
        self.create_entity_kind("Abstract") # TODO: merge it all in one-line code: create [entity_kinds, properties, relationships]
        self.create_property_kind_of_entity_kind("Abstract", "Name", str) # TODO: make it mandatory
        self.create_relationship_kind("Abstract", "HAS_PARENT", "Abstract")

    def _create_abstract_instances(self, entity_kinds: List[str], parents: List[Union[str, None]]):
        entity_ids = ["Abstract/"+kind for kind in entity_kinds]
        self.kg.create_entities(
            entity_kinds=["Abstract"]*len(entity_kinds),
            entity_ids=entity_ids,
            property_kinds=[["Name"]]*len(entity_kinds),
            property_values=[[kind] for kind in entity_kinds]
        )
        existing_parents = parents.copy()
        entity_with_parents = entity_ids.copy()
        for entity_id, parent in zip(entity_ids, parents):
            if parent is None:
                existing_parents.remove(parent)
                entity_with_parents.remove(entity_id)
        existing_parents = ["Abstract/"+parent for parent in existing_parents]
        if existing_parents:
            self.kg.create_relationships(
                entity_with_parents, ["HAS_PARENT"]*len(existing_parents), existing_parents
            )
        return entity_ids

    def _form_property_uri(self, entity_kind, property, prop_type="string", type_family="Optional"):
        uri = f"@schema:{entity_kind}/{property}/{type_family}+xsd%3A{prop_type}"
        return uri

    def _form_relationship_uri(self, entity_kind, relationship, related_kind):
        uri = f"@schema:{entity_kind}/{relationship}/Set+{related_kind}"
        return uri    

    def _get_schema(self):
        ttl_schema = self._client.get_triples("schema")
        ttl_schema = "\n".join([
            ttl_schema[:ttl_schema.rfind("\n\n")] ,
            """
                <terminusdb://context>
                a sys:Context ;
                sys:base "terminusdb:///data/"^^xsd:string ;
                sys:schema "terminusdb:///schema#"^^xsd:string .
            """
        ])
        return ttl_schema

    @staticmethod
    def _type2str(types_to_convert: List[Type]) -> List[str]:
        """Converts list of types to a list of strings."""
        types_str = []
        types = {
            str: "string",
            int: "integer",
            float: "decimal",
            bool: "boolean",
            datetime.date: "date",
            datetime.time: "time",
            datetime.datetime: "datetime",
            list: "List",
            set: "Set",
            Optional: "Optional",
            "Mandatory": "Mandatory",
        }
        for item in types_to_convert:
            types_str.append(types.get(item))
        return types_str

    def _rel_kinds2full_qualified_rel_kinds(self, relationship_kinds: List[str], kinds_b: List[str]) -> List[str]:
        return ["".join([rel_kind, "/", kind_b]) for rel_kind, kind_b in zip(relationship_kinds, kinds_b)]

    def _full_qualified_rel_kind2rel_kind(self, full_qualified_relationship_kind: str) -> str:
        return full_qualified_relationship_kind.split("/")[0]

    def _get_kinds_out_of_ids(self, ids: List[str]) -> List[str]:
        return [id.split("/")[0] for id in ids]

    def _get_relationship_kinds_by_labels_and_entity_kinds(self, kinds_a, relationship_labels, kinds_b):
        assert len(kinds_a) == len(relationship_labels) == len(kinds_b), (
            "Number of kinds_a doesn't correspond properly with number of"
                " kinds_b or relationship kinds. They should be equal"
        )
        allowed_rels = self._get_relationship_kinds_by_labels(relationship_labels)
        allowed_relationships = {}
        # organizing allowed_rels
        for dic in allowed_rels:
            if (dic["kind_a"], dic["kind_b"]) not in allowed_relationships:
                allowed_relationships[(dic["kind_a"], dic["kind_b"])] = [dic["rel"]]
            else:
                allowed_relationships[(dic["kind_a"], dic["kind_b"])].append(dic["rel"])
        
        parents = self._get_parents_of_entity_kinds(kinds_a + kinds_b)

        transformed_relationships = []
        for (kind_a, relationship_label, kind_b) in zip(kinds_a, relationship_labels, kinds_b):
            if (kind_a, kind_b) in allowed_relationships:
                for rel in allowed_relationships[(kind_a, kind_b)]:
                    if rel.startswith(relationship_label):
                        transformed_relationships.append(rel)
                        break
                else: # if no relationship from base starts with some relationship_label
                    raise ValueError(f"There's no such relationship like '{relationship_label}'. allowed_relationships: '{allowed_relationships}'")
                continue

            rel_terminals = {
                "kinds_a": parents[kind_a] + [kind_a] if kind_a in parents else [kind_a],
                "kinds_b": parents[kind_b] + [kind_b] if kind_b in parents else [kind_b]
            }

            for kind_a in rel_terminals["kinds_a"]:
                for kind_b in rel_terminals["kinds_b"]:
                    if (kind_a, kind_b) in allowed_relationships:
                        for rel in allowed_relationships[(kind_a, kind_b)]:
                            if rel.startswith(relationship_label):
                                transformed_relationships.append(rel)
                                break
                        else: # if no relationship from base starts with some relationship_label
                            raise ValueError(f"There's no such relationship like '{relationship_label}'. allowed_relationships: '{allowed_relationships}'")
                        break
                else:
                    continue
                break
            else: # if the loop has finished without any 'append' then the (kind_a, kind_b) isn't allowed
                raise ValueError(f"There's no relationship kind between '{(kind_a, kind_b)}'. allowed_relationships: '{allowed_relationships}'")
        return transformed_relationships

    def _get_parents_of_entity_kinds(self, entity_kinds: List[str]):
        entity_kind = entity_kinds.pop(0)
        query = WOQL().quad(f"@schema:{entity_kind}", "sys:inherits", f"v:{entity_kind}", "schema")
        for entity_kind in entity_kinds:
            query = WOQL().woql_or(
            query,
            WOQL().quad(f"@schema:{entity_kind}", "sys:inherits", f"v:{entity_kind}", "schema")
            )
        results = query.execute(self._client)
        pretty_results = {}
        for dic in results["bindings"]:
            for k, v in dic.items():
                if v is not None:
                    if k not in pretty_results:
                        pretty_results[k] = [v.split(":")[-1]]
                    else:
                        pretty_results[k].append(v.split(":")[-1])
                    #return a list of parents not only the last appended one )
        return pretty_results

    def create_entity_kinds(self, entity_kinds: List[str], parents: Optional[List[Union[str, None]]] = None):
        if parents is None:
            parents = [None]*len(entity_kinds)

        query = WOQL().woql_and(*[
            WOQL().add_quad(":".join(["@schema", entity_kind]), "rdf:type", "sys:Class", "schema") for entity_kind in entity_kinds
        ])

        query = WOQL().woql_and(
            query,
            *[
                WOQL().add_quad(":".join(["@schema", entity_kind]), "sys:inherits", ":".join(["@schema", parent]), "schema")
                for entity_kind, parent in zip(entity_kinds, parents) if parent is not None
            ]
        )
        result = query.execute(self._client)

        if result == "Commit successfully made.":
            if entity_kinds!=["Abstract"]:
                abstract_kinds_instances = self._create_abstract_instances(entity_kinds, parents)
            else:
                abstract_kinds_instances = None

            return abstract_kinds_instances
        elif result["api:status"] == "api:success":
            logging.info("Abstract kind is already in database")
        else:
            raise DatabaseError(f"failed to commit to schema, message: {result['api:status']}")

    def create_entity_kind(self, entity_kind: str, parent: Optional[str] = None):
        return self.create_entity_kinds([entity_kind], [parent])

    def update_label_of_entity_kind(self, entity_kind: str, label: str):
        query = WOQL().woql_and(
            WOQL().update_quad(f"@schema:{entity_kind}", "sys:documentation", f"@schema:{entity_kind}/0/documentation/Documentation", "schema"),
            WOQL().update_quad(f"@schema:{entity_kind}/0/documentation/Documentation", "rdf:type", "sys:Documentation", "schema"),
            WOQL().update_quad(f"@schema:{entity_kind}/0/documentation/Documentation", "sys:comment", {'@type': "xsd:string", "@value": label}, "schema"),
        )
        return query.execute(self._client)

    def delete_entity_kinds(self, entity_kinds: List[str]):
        # delete from Abstract
        entity_ids = [f"Abstract/{entity_kind}" for entity_kind in entity_kinds]
        self.kg.delete_entities(entity_ids)

        query = WOQL().quad("v:a", "v:r", "v:b", "schema")
        for entity_kind in entity_kinds:
            query = WOQL().woql_and(
                query,
                WOQL().woql_or(
                    # delete the class definition
                    WOQL().quad(
                        f"@schema:{entity_kind}", f"rdf:type", f"sys:Class", "schema"
                    ).delete_quad(
                        f"@schema:{entity_kind}", f"rdf:type", f"sys:Class", "schema"
                    ),
                    # delete the class properties definitions
                    WOQL().quad(
                        f"@schema:{entity_kind}", f"v:property_{entity_kind}", f"v:property_def_{entity_kind}", "schema"
                    ).delete_quad(
                        f"@schema:{entity_kind}", f"v:property_{entity_kind}", f"v:property_def_{entity_kind}", "schema"
                    ).quad(
                        f"v:property_def_{entity_kind}", f"sys:class", f"v:property_def_value_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:property_def_{entity_kind}", f"sys:class", f"v:property_def_value_{entity_kind}", "schema"
                    ).quad(
                        f"v:property_def_{entity_kind}", f"rdf:type", f"v:property_type_family_value_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:property_def_{entity_kind}", f"rdf:type", f"v:property_type_family_value_{entity_kind}", "schema"
                    ),
                    # delete the children of class
                    WOQL().quad(
                        f"v:child_{entity_kind}", "sys:inherits", f"@schema:{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:child_{entity_kind}", "sys:inherits", f"@schema:{entity_kind}", "schema"
                    ),
                    # delete the parents of class
                    WOQL().quad(
                        f"@schema:{entity_kind}", "sys:inherits", f"v:parent_{entity_kind}", "schema"
                    ).delete_quad(
                        f"@schema:{entity_kind}", "sys:inherits", f"v:parent_{entity_kind}", "schema"
                    ),
                    # delete documentation of properties of class
                    WOQL().quad(
                        f"@schema:{entity_kind}", "sys:documentation", f"v:doc_{entity_kind}", "schema"
                    ).delete_quad(
                        f"@schema:{entity_kind}", "sys:documentation", f"v:doc_{entity_kind}", "schema"
                    ).quad(
                        f"v:doc_{entity_kind}", f"rdf:type", f"sys:Documentation", "schema"
                    ).delete_quad(
                        f"v:doc_{entity_kind}", f"rdf:type", f"sys:Documentation", "schema"
                    ).quad(
                        f"v:doc_{entity_kind}", f"sys:properties", f"v:class_property_doc_def_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:doc_{entity_kind}", f"sys:properties", f"v:class_property_doc_def_{entity_kind}", "schema"
                    ).quad(
                        f"v:class_property_doc_def_{entity_kind}", f"v:prop_label_and_prop_doc_def{entity_kind}", f"v:prop_label&def_values_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:class_property_doc_def_{entity_kind}", f"v:prop_label_and_prop_doc_def{entity_kind}", f"v:prop_label&def_values_{entity_kind}", "schema"
                    ),
                    # delete backward relationship definitions from kind_a
                    WOQL().quad(
                        f"v:rel_def_{entity_kind}", "sys:class", f"@schema:{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:rel_def_{entity_kind}", "sys:class", f"@schema:{entity_kind}", "schema"
                    ).quad(
                        f"v:kind_a_{entity_kind}", f"v:rel_{entity_kind}", f"v:rel_def_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:kind_a_{entity_kind}", f"v:rel_{entity_kind}", f"v:rel_def_{entity_kind}", "schema"
                    ).quad(
                        f"v:rel_def_{entity_kind}", f"v:rel_def_family_type_{entity_kind}", f"v:family_type_value_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:rel_def_{entity_kind}", f"v:rel_def_family_type_{entity_kind}", f"v:family_type_value_{entity_kind}", "schema"
                    # delete documentation of a backward relationship from kind_a
                    ).quad(
                        f"v:kind_a_{entity_kind}", "sys:documentation", f"v:doc_a_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:kind_a_{entity_kind}", "sys:documentation", f"v:doc_a_{entity_kind}", "schema"
                    ).quad(
                        f"v:doc_a_{entity_kind}", "sys:properties", f"v:property_doc_def_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:doc_a_{entity_kind}", "sys:properties", f"v:property_doc_def_{entity_kind}", "schema"
                    ).quad(
                        f"v:property_doc_def_{entity_kind}", f"v:rel_label_and_rel_doc_def", f"v:rel_label&def_values_{entity_kind}", "schema"
                    ).delete_quad(
                        f"v:property_doc_def_{entity_kind}", f"v:rel_label_and_rel_doc_def", f"v:rel_label&def_values_{entity_kind}", "schema"
                    )
                )
            )
        result = query.execute(self._client)
        if result == "Commit successfully made.":
            return result
        elif not result.get("bindings"):
            raise ValueError("Something wrong with the entered entity kinds")

    def delete_entity_kind(self, entity_kind: str):
        return self.delete_entity_kinds([entity_kind])

    def get_all_entity_kinds(self):
        """Returns all entity kinds with their direct properties in addition to the parent classes if exist"""
        return self._client.get_existing_classes()

    def get_entity_kind(self, entity_kind: str):
        """Returns entity kind direct properties as well as inherited ones from parents"""
        return self._client.get_class_frame(entity_kind)

    def create_property_kinds_of_entity_kinds(
        self,
        entity_kinds: List[str],
        property_kinds: List[List[str]],
        property_types: Optional[List[List[Type]]]= None,
        properties_type_families: Optional[List[List[Type]]] = None,
    ):
        if properties_type_families is None:
            properties_type_families = [[None]*len(prop_kinds) for prop_kinds in property_kinds]
        if property_types is None:
            property_types = [[None]*len(prop_kinds) for prop_kinds in property_kinds]

        query = WOQL().quad("v:a", "v:r", "v:c", "schema")
        for (
            entity_kind,
            property_kinds_for_this_entity_kind,
            property_types_for_this_entity_kind,
            properties_type_families_for_this_entity_kind,
        ) in zip(
            entity_kinds,
            property_kinds,
            property_types,
            properties_type_families,
        ):
            property_types_for_this_entity_kind = list(
                map(lambda x: x if x is not None else str, property_types_for_this_entity_kind)
            )
            properties_type_families_for_this_entity_kind = list(
                map(lambda x: x if x is not None else Optional, properties_type_families_for_this_entity_kind)
            )
            property_types_for_this_entity_kind = self._type2str(property_types_for_this_entity_kind)
            properties_type_families_for_this_entity_kind = self._type2str(properties_type_families_for_this_entity_kind)

            for prop_kind, prop_type, type_family in zip(
                property_kinds_for_this_entity_kind, property_types_for_this_entity_kind, properties_type_families_for_this_entity_kind
            ):
                uri = self._form_property_uri(entity_kind, prop_kind, prop_type, type_family)
                query = WOQL().woql_and(
                    query,
                    WOQL().add_quad(":".join(["@schema", entity_kind]), ":".join(["@schema", prop_kind]), uri, "schema"),
                    WOQL().add_quad(uri, "sys:class", f"xsd:{prop_type}", "schema"),
                    WOQL().add_quad(uri, "rdf:type", f"sys:{type_family}", "schema"),
                )
        return query.execute(self._client)

    def create_property_kinds_of_entity_kind(
        self,
        entity_kind: str,
        property_kinds: List[str],
        property_types: Optional[List[Type]]= None,
        properties_type_families: Optional[List[Type]] = None,
    ):
        if property_types is not None:
            property_types = [property_types]
        if properties_type_families is not None:
            properties_type_families = [properties_type_families]

        return self.create_property_kinds_of_entity_kinds(
            [entity_kind],
            [property_kinds],
            property_types,
            properties_type_families,
        )

    def create_property_kind_of_entity_kind(
        self,
        entity_kind: str,
        property_kind: str,
        property_type: Optional[Type] = None,
        property_type_family: Optional[Type] = None,
    ):
        if property_type is not None:
            property_type = [property_type]
        if property_type_family is not None:
            property_type_family = [property_type_family]
        return self.create_property_kinds_of_entity_kind(
            entity_kind, [property_kind], property_type, property_type_family
        )

    def update_labels_of_property_kinds(self, entity_kinds: List[str], property_kinds: List[str], labels: List[str]):
        query = WOQL().quad("v:a", "v:r", "v:b", "schema")
        for entity_kind, property_kind, label in zip(entity_kinds, property_kinds, labels):
            query = WOQL().woql_and(
                query,
                WOQL().update_quad(f"@schema:{entity_kind}", "sys:documentation", f"@schema:{entity_kind}/0/documentation/Documentation", "schema"),
                WOQL().update_quad(f"@schema:{entity_kind}/0/documentation/Documentation", "rdf:type", "sys:Documentation", "schema"),
                WOQL().update_quad(f"@schema:{entity_kind}/0/documentation/Documentation", "sys:properties", f"@schema:{entity_kind}/0/documentation/Documentation/properties/{property_kind}", "schema"),
                WOQL().update_quad(f"@schema:{entity_kind}/0/documentation/Documentation/properties/{property_kind}", "rdf:type", "sys:PropertyDocumentation", "schema"),
                WOQL().update_quad(f"@schema:{entity_kind}/0/documentation/Documentation/properties/{property_kind}", f"@schema:{property_kind}", {'@type': "xsd:string", "@value": label}, "schema"),
            )    
        return query.execute(self._client)

    def update_label_of_property_kind(self, entity_kind: str, property_kind: str, label: str):
        return self.update_labels_of_property_kinds([entity_kind], [property_kind], [label])

    def delete_property_kinds(self, entity_kind: str, property_kinds: List[str]):
        query = WOQL().woql_and(
            *[
                WOQL().quad(
                    f"@schema:{entity_kind}", f"@schema:{property_kind}", f"v:variable_{property_kind}", "schema"
                ).delete_quad(f"@schema:{entity_kind}", f"@schema:{property_kind}", f"v:variable_{property_kind}", "schema")
                for property_kind in property_kinds
            ]
        )
        return query.execute(self._client)

    def delete_property_kind(self, entity_kind: str, property_kind: str):
        return self.delete_property_kinds(entity_kind, [property_kind])

    def create_relationship_kinds(
        self,
        entity_kinds_a: List[str],
        relationship_kinds: List[str],
        entity_kinds_b: List[str],
    ):
        relationship_kind_labels = relationship_kinds.copy()
        relationship_kinds = self._rel_kinds2full_qualified_rel_kinds(relationship_kinds, entity_kinds_b)
        ttl_schema_parts = []

        query = WOQL().quad("v:a", "v:r", "v:c", "schema")
        for entity_kind_a, rel_kind, entity_kind_b in zip(
                entity_kinds_a, relationship_kinds, entity_kinds_b 
            ):
                uri = self._form_relationship_uri(entity_kind_a, rel_kind, entity_kind_b)
                query = WOQL().woql_and(
                    query,
                    WOQL().add_quad(":".join(["@schema", entity_kind_a]), ":".join(["@schema", rel_kind]), uri, "schema"),
                    WOQL().add_quad(uri, "sys:class", f"@schema:{entity_kind_b}", "schema"),
                    WOQL().add_quad(uri, "rdf:type", f"sys:Set", "schema"),
                )
        query.execute(self._client)
        return self.update_labels_of_property_kinds(entity_kinds_a, relationship_kinds, relationship_kind_labels)

    def create_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        return self.create_relationship_kinds([entity_kind_a], [relationship_kind], [entity_kind_b])

    def get_relationship_kind(self, relationship_kind: str):
        if relationship_kind.startswith("@"):
            raise ValueError(f"'{relationship_kind}' is not a relationship kind")
        relationship_details = []
        for entity_kind, props in self.get_all_entity_kinds().items():
            if relationship_kind in props:
                relationship_details.append((entity_kind, props[relationship_kind]["@class"]))
        return relationship_details

    def _get_relationship_kinds_by_labels(self, labels: List[str]) -> List[dict]:
        labels = labels.copy()
        kind_b = {'@type': 'xsd:string', '@value': labels.pop(0)}
        query = WOQL().select("kind_a", "rel", "kind_b").quad("v:kind_a", "v:rel", "v:kind_b", "schema").quad("v:props_with_labels", "v:rel", kind_b, "schema")
        for label in labels:
            kind_b = {'@type': 'xsd:string', '@value': label}
            query = WOQL().woql_or(
                query,
                WOQL().select("kind_a", "rel", "kind_b").quad("v:kind_a", "v:rel", "v:kind_b", "schema").quad("v:props_with_labels", "v:rel", kind_b, "schema"),
            )
        results = query.execute(self._client)
        relationships = [triple for triple in results["bindings"] if "documentation" not in triple["kind_a"]]
        for relationship in relationships:
            relationship["kind_a"] = relationship["kind_a"].split(":")[-1]
            relationship["rel"] = relationship["rel"].split(":")[-1]
            relationship["kind_b"] = relationship["kind_b"].split("+")[-1]
        relationships = [dict(triple) for triple in {tuple(relationship.items()) for relationship in relationships}] # to delete duplicates
        return relationships

    def delete_relationship_kinds(self, entity_kinds_a: List[str], relationship_kinds: List[str], entity_kinds_b: List[str]):
        relationship_kinds = self._get_relationship_kinds_by_labels_and_entity_kinds(
            entity_kinds_a, relationship_kinds, entity_kinds_b
        )
        query = WOQL().woql_and(
            *[
                WOQL().quad(
                    f"@schema:{entity_kind_a}", f"@schema:{relationship_kind}", f"v:variable_{relationship_kind}", "schema"
                ).delete_quad(
                    f"@schema:{entity_kind_a}", f"@schema:{relationship_kind}", f"v:variable_{relationship_kind}", "schema"
                ).quad(
                    f"@schema:{entity_kind_a}", "sys:documentation", f"v:doc_{relationship_kind}", "schema"
                ).quad(
                    f"v:doc_{relationship_kind}", "sys:properties", f"v:rel_{relationship_kind}", "schema"
                ).quad(
                    f"v:rel_{relationship_kind}", f"@schema:{relationship_kind}", f"v:label_value_{relationship_kind}", "schema"
                ).delete_quad(
                    f"v:rel_{relationship_kind}", f"@schema:{relationship_kind}", f"v:label_value_{relationship_kind}", "schema"
                )
                for entity_kind_a, relationship_kind in zip(entity_kinds_a, relationship_kinds)
            ]
        )
        return query.execute(self._client)

    def delete_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        return self.delete_relationship_kinds([entity_kind_a], [relationship_kind], [entity_kind_b])
