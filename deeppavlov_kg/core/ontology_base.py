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
        pass

    def create_entity_kind(self, entity_kind: str):
        pass

    def delete_entity_kind(self, entity_kind: str):
        pass

    def create_property_kinds(self, entity_kinds: List[str], property_kinds: List[str], property_types: Optional[List[Type]]= None):
        pass

    def create_property_kind(self, entity_kind: str, property_kind: str, property_type: Type):
        pass

    def delete_property_kinds(self, entity_kinds: List[str], property_kinds: List[str]):
        pass

    def delete_property_kind(self, entity_kind: str, property_kind: str):
        pass

    def create_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str], entity_kinds_b: List[str]):
        pass

    def create_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        pass

    def delete_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        pass

    # bonus
    def create_relationship_property_kind():
        pass

    # bonus
    def update_relationship_property_kind():
        pass


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
        kind_properties: Optional[List[str]] = None,
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
            kind_properties = []
        if kind_property_types is None:
            kind_property_types = [str] * (len(kind_properties))
        if kind_property_measurement_units is None:
            kind_property_measurement_units = [""] * (len(kind_properties))

        if len(kind_property_types) != len(kind_property_measurement_units):
            logging.error(
                "Number of property types doesn't correspond properly with number of"
                " property measurement_units. They should be equal"
            )

        kind = kind.capitalize()
        parent = parent.capitalize()

        if start_tree is None:
            start_tree = self._load_ontology_kinds_hierarchy()
            if start_tree is None:
                start_tree = Tree()
                start_tree.create_node(
                    tag="Kind",
                    identifier="Kind",
                    data=Kind(
                        {
                            "_deleted": {"type": str(bool), "measurement_unit": ""},
                        }
                    ),
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

    def create_property_kinds(
        self,
        kind: str,
        new_property_kinds: List[str],
        new_property_types: Optional[List[Type]] = None,
        # new_property_measurement_units: Optional[List[str]] = None,
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
        # if new_property_measurement_units is None:
        #     new_property_measurement_units = [""] * len(new_property_kinds)

        if not (
            len(new_property_kinds)
            == len(new_property_types)
            # == len(new_property_measurement_units)
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
                        # "measurement_unit": new_property_measurement_units[idx],
                    }
                }
            )

        self._save_ontology_kinds_hierarchy(tree)
        logging.info("Properties has been updated successfully")
        return kind_node

    def create_property_kind(self, entity_kind: str, property_kind: str, property_type: Type):
        self.create_property_kinds(entity_kind, [property_kind], [property_type])

    def delete_property_kinds(self, entity_kinds: List[str], property_kinds: List[str]):
        raise NotImplementedError

    def delete_property_kind(self, entity_kind: str, property_kind: str):
        raise NotImplementedError

    def create_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str], entity_kinds_b: List[str]):
        raise NotImplementedError

    def create_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
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

    def delete_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        raise NotImplementedError

    # bonus
    def create_relationship_property_kind(self):
        raise NotImplementedError

    # bonus
    def update_relationship_property_kind(self):
        raise NotImplementedError

    # bonus
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
    ):
        self.client = client

    def _form_property_uri(self, entity_kind, property, prop_type="string"): # TODO: make it private
        uri = f"<schema#{entity_kind}/{property}/Optional+xsd%3A{prop_type}> "
        return uri

    def _form_relationship_uri(self, entity_kind, relationship, related_kind):
        uri = f"<schema#{entity_kind}/{relationship}/Optional+{related_kind}> "
        return uri    

    def _get_schema(self):
        ttl_schema = self.client.get_triples("schema")
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

    def _create_or_update_schema(
        self,
        entity_kind: str,
        property_kinds: Optional[List[str]] = None,
        property_types: Optional[List[Type]] = None,
        properties_optionality: Optional[List[bool]] = None, # TODO: optional, set, mandatory, or list
        relationship_kinds: Optional[List[Tuple[str, str]]] = None,
        relationships_optionality: Optional[List[bool]] = None, # TODO: optional, set, mandatory, or list
    ):
        # TODO: check if the relationship kind exist raise an error
        # properties processing
        if property_kinds is not None:
            if property_types is not None:
                if len(property_types) != len(property_kinds):
                    logging.error(
                        "Number of property types doesn't correspond properly with number of"
                        " property kinds. They should be equal"
                    )
            else:
                property_types = [str] * len(property_kinds)
            
            if property_types is not None:
                property_types = TerminusdbOntologyConfig._type2str(property_types)
            
            if properties_optionality is not None:
                if len(properties_optionality) != len(property_kinds):
                    logging.error(
                        "Number of property optionality items doesn't correspond properly with number of"
                        " property kinds. They should be equal"
                    )
                for option in properties_optionality:
                    if option not in [True, False]:
                        logging.error(
                            "Unknown optionality value. Should be one of %s", "[True, False]"
                        )
            else:
                properties_optionality = [True] * len(property_kinds)

            prop_definitions = []
            properties = []
            for prop, type, optional in zip(property_kinds, property_types, properties_optionality):
                if optional:
                    prop_uri = self._form_property_uri(entity_kind, prop, type)
                    prop_addition = f"""
                        <schema#{prop}> {prop_uri}
                    """
                    prop_definition = f"""
                        {prop_uri}
                          a sys:Optional ;
                          sys:class xsd:{type} .
                    """
                else:
                    prop_addition = f"""
                        <schema#{prop}> xsd:{type} 
                    """
                    prop_definition = ""
                properties.append(prop_addition)
                prop_definitions.append(prop_definition)
            ttl_properties = " ;\n".join(properties)
            ttl_prop_definitions = "\n".join(prop_definitions)
        else:
            ttl_properties = ""
            ttl_prop_definitions = ""

        # Relationships processing
        if relationship_kinds:
            if relationships_optionality is not None:
                if len(relationships_optionality) != len(relationship_kinds):
                    logging.error(
                        "Number of relationship optionality items doesn't correspond properly with number of"
                        " relationship kinds. They should be equal"
                    )
                for option in relationships_optionality:
                    if option not in [True, False]:
                        logging.error(
                            "Unknown optionality value. Should be one of %s", "[True, False]"
                        )
            else:
                relationships_optionality = [True] * len(relationship_kinds)

            rel_additions = []
            rel_definitions = []
            for (relationship_kind, related_kind), optional in zip(relationship_kinds, relationships_optionality):
                rel_uri = self._form_relationship_uri(entity_kind, relationship_kind, related_kind)
                if optional:
                    rel_addition = f"""
                        <schema#{relationship_kind}> {rel_uri} 
                    """ 

                    rel_definition = f"""
                        {rel_uri}
                            a sys:Optional ;
                            sys:class <schema#{related_kind}> .
                    """
                else:
                    rel_addition = f"""
                        <schema#{relationship_kind}> <schema#{related_kind}>
                    """
                    rel_definition = ""
                rel_additions.append(rel_addition)
                rel_definitions.append(rel_definition)
            ttl_relationships = " ;\n".join(rel_additions)
            ttl_rel_definitions = "\n".join(rel_definitions)
        else:
            ttl_relationships = ""
            ttl_rel_definitions = ""

        ttl_schema = f"""
            @base <terminusdb:///schema#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix woql: <http://terminusdb.com/schema/woql#> .
            @prefix json: <http://terminusdb.com/schema/json#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            @prefix xdd: <http://terminusdb.com/schema/xdd#> .
            @prefix vio: <http://terminusdb.com/schema/vio#> .
            @prefix sys: <http://terminusdb.com/schema/sys#> .
            @prefix api: <http://terminusdb.com/schema/api#> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            @prefix doc: <data/> .
            <schema#{entity_kind}>
            a sys:Class ;
              {ttl_properties}
              {ttl_relationships} .
            {ttl_prop_definitions}
            {ttl_rel_definitions}
            <terminusdb://context>
            a sys:Context ;
            sys:base "terminusdb:///data/"^^xsd:string ;
            sys:schema "terminusdb:///schema#"^^xsd:string .
        """
        return self.client.insert_triples(
            graph_type='schema',
            content=ttl_schema,
            commit_msg="Insert triples"
        )

    def _delete_from_schema(
        self,
        entity_kind: str,
        property_kinds: List[str],
        # entity_kind_b: str = "All",
    ):
        ttl_schema = self._get_schema()
        instructions = ttl_schema.split(" .")
        new_instructions = []
        for instruction in instructions:
            for property_kind in property_kinds:
                # delete definition
                if (
                    f"<schema#{entity_kind}/{property_kind}/" in instruction 
                    and "a sys:Class" not in instruction
                ):
                    continue
                # delete mention
                elif f"<schema#{entity_kind}>" in instruction and "a sys:Class" in instruction:
                    # if entity_kind_b != "All":
                    #     pattern_of_rel_with_kind_b = f".*<schema#\w*/{property_kind}/\w*\+{entity_kind_b}>.*"
                    #     if re.match(pattern_of_rel_with_kind_b, instruction, re.DOTALL) is not None:
                    #         # then delete only this line from the instruction
                    #         instruction = "".join(re.split(pattern_of_rel_with_kind_b, instruction))
                    # else: # if "All" and this's relationship, it will delete relationships between entity_kind and all existing entity_kind_b. if a property, then it'll delete every mention of this property.
                    prop_definition_pattern = f"<schema#{property_kind}>.*;"
                    instruction = "".join(re.split(prop_definition_pattern, instruction))
                new_instructions.append(instruction)
        ttl_schema = ".".join(new_instructions)

        try:
            return self.client.update_triples(graph_type='schema', content=ttl_schema, commit_msg="Insert triples")
        except DatabaseError:
            logging.error("Most likely, you're trying to delete a property that already has instances for some documents.")

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
        }
        for item in types_to_convert:
            types_str.append(types.get(item))
        return types_str

    def create_entity_kind(self, entity_kind: str):
        return self._create_or_update_schema(entity_kind)

    def delete_entity_kind(self, entity_kind: str):
        ttl_schema = self._get_schema()
        instructions = ttl_schema.split(" .")
        new_instructions = []
        pattern_of_rel_with_kind_b_equal_entity_kind = f".*<schema#\w*/\w*/\w*\+{entity_kind}>.*"
        for instruction in instructions:
            if (
                (f"<schema#{entity_kind}>" in instruction and "a sys:Class" in instruction) # the entity_kind class definition
                or (f"<schema#{entity_kind}" in instruction and "a sys:Class" not in instruction) # definitions of properties of entity_kind class
                or (re.match(pattern_of_rel_with_kind_b_equal_entity_kind, instruction, re.DOTALL) is not None and "a sys:Class" not in instruction) # definitions of relationships that have their entity_kind_b=entity_kind
            ):
                continue
            elif re.match(pattern_of_rel_with_kind_b_equal_entity_kind, instruction, re.DOTALL) is not None and "a sys:Class" in instruction: # entity_kind mentions in other classes definitions where relationship entity_kind_b=entity_kind
                # then delete only this line from the instruction
                instruction = "".join(re.split(pattern_of_rel_with_kind_b_equal_entity_kind, instruction))
            new_instructions.append(instruction)
        ttl_schema = ".".join(new_instructions)
        try:
            return self.client.update_triples(graph_type='schema', content=ttl_schema, commit_msg="Insert triples")
        except DatabaseError:
            logging.error("Most likely, you're trying to delete a property that already has instances for some documents.")

    def create_property_kinds(self, entity_kind: str, property_kinds: List[str], property_types: Optional[List[Type]]= None):
        return self._create_or_update_schema(
            entity_kind,
            property_kinds,
            property_types,
        )

    def create_property_kind(self, entity_kind: str, property_kind: str, property_type: Type):
        return self.create_property_kinds(entity_kind, [property_kind], [property_type])

    def delete_property_kinds(self, entity_kind: str, property_kinds: List[str]):
        return self._delete_from_schema(entity_kind, property_kinds)

    def delete_property_kind(self, entity_kind: str, property_kind: str):
        return self.delete_property_kinds(entity_kind, [property_kind])

    def create_relationship_kinds(
        self,
        entity_kind_a: str,
        relationship_kinds: List[str],
        entity_kinds_b: List[str],
    ):
        return self._create_or_update_schema(
                entity_kind_a,
                relationship_kinds=list(zip(relationship_kinds, entity_kinds_b)),
            )

    def create_relationship_kind(self, entity_kind_a: str, relationship_kind: str, entity_kind_b: str):
        return self.create_relationship_kinds(entity_kind_a, [relationship_kind], [entity_kind_b])

    def delete_relationship_kinds(self, entity_kind_a: str, relationship_kinds: List[str]):
        self._delete_from_schema(entity_kind_a, relationship_kinds)

    def delete_relationship_kind(self, entity_kind_a: str, relationship_kind: str):
        return self.delete_relationship_kinds(entity_kind_a, [relationship_kind])
