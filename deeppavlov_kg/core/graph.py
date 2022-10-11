import os
from pathlib import Path
from typing import Any, Optional, List, Tuple, Union
import logging
import datetime
from neomodel import db, config, clear_neo4j_database
from neo4j import graph as neo4j_graph
from neo4j.exceptions import ClientError
from deeppavlov_kg.core.ontology import Neo4jOntologyConfig, TerminusdbOntologyConfig
from deeppavlov_kg.core import querymaker

from terminusdb_client import WOQLClient, WOQLQuery as WOQL
from terminusdb_client.errors import InterfaceError, DatabaseError

class KnowledgeGraph:
    def __init__(
        self,
        database: str = "terminusdb",
        neo4j_bolt_url: str = "",
        ontology_kinds_hierarchy_path: Union[Path, str] = "",
        ontology_data_model_path: Union[Path, str] = "",
        db_ids_file_path: Union[Path, str] = "",
        team: str = "",
        db_name: str = "",
    ):
        if database == "terminusdb":
            if not (team and db_name):
                raise ValueError("team and db_name should be provided when the chosen database is 'terminusdb'")
            endpoint = f"https://cloud.terminusdb.com/{team}/"
            self._client   = WOQLClient(endpoint)
            try:
                self._client.connect(team=team, use_token=True, db=db_name)
            except InterfaceError:
                self._client.connect(team=team, use_token=True)
                self._client.create_database(db_name)
            self.ontology = TerminusdbOntologyConfig(self._client)

        elif database == "neo4j":
            if not (
                neo4j_bolt_url
                and ontology_kinds_hierarchy_path
                and ontology_data_model_path
                and db_ids_file_path
            ):
                raise ValueError(
                    "When the chosen database is neo4j, neo4j_bolt_url, "
                    "ontology_kinds_hierarchy_path, ontology_data_model_path, "
                    "and db_ids_file_path should be provided"
                )
            config.DATABASE_URL = neo4j_bolt_url
            self.ontology = Neo4jOntologyConfig(
                ontology_kinds_hierarchy_path, ontology_data_model_path
            )
            self.db_ids_file_path = Path(db_ids_file_path)
        else:
            raise ValueError("database only could be either 'terminusdb' or 'neo4j'")

    @classmethod # try to delete from_obj! what happens ))
    def from_obj(cls, config_obj):
        return cls(
            neo4j_bolt_url=config_obj.neo4j_bolt_url,
            ontology_kinds_hierarchy_path=config_obj.ontology_kinds_hierarchy_path,
            ontology_data_model_path=config_obj.ontology_data_model_path,
            db_ids_file_path=config_obj.db_ids_file_path,
            team=config_obj.team,
            db_name=config_obj.db_name,
        )

    def drop_database(self):
        raise NotImplementedError

    def create_entity(self, kind: str, entity_id: str, property_kinds: List[str], property_values: list):
        raise NotImplementedError

    def delete_entity(self, entity_id: str):
        raise NotImplementedError

    def create_or_update_properties_of_entities(self, entity_ids: List[str], property_kinds: List[str], new_property_values: List[Any]):
        raise NotImplementedError
    
    def create_or_update_properties_of_entity(self, entity_id: str, property_kinds: List[str], new_property_values: List[Any]):
        raise NotImplementedError
    
    def create_or_update_property_of_entity(self, entity_id: str, property_kind: str, new_property_values: Any):
        raise NotImplementedError

    def delete_properties_from_entities(self, entity_ids: List[str], property_kinds: List[str]):
        raise NotImplementedError
    
    def delete_properties_from_entity(self, entity_id: str, property_kinds: List[str]):
        raise NotImplementedError

    def delete_property_from_entity(self, entity_id: str, property_kind: str):
        raise NotImplementedError

    def get_all_entities(self):
        raise NotImplementedError

    def get_properties_of_entity(self, entity_id: str):
        raise NotImplementedError

    def create_relationship(self, id_a: str, relationship_kind: str, id_b: str):
        raise NotImplementedError

    def search_for_relationships(
        self,
        relationship_kind: str,
        id_a: Optional[str] = None,
        id_b: Optional[str] = None,
    ):
        raise NotImplementedError

    def delete_relationship(self, id_a: str, relationship_kind: str, id_b: str):
        raise NotImplementedError

    def get_entities_by_date(self, entity_ids: List[str], date_to_inspect: datetime.datetime):
        raise NotImplementedError

    def get_entity_by_date(self, entity_id: str, date_to_inspect: datetime.datetime):
        raise NotImplementedError

class Neo4jKnowledgeGraph(KnowledgeGraph):
    def __init__(
        self,
        neo4j_bolt_url: str,
        ontology_kinds_hierarchy_path: Union[Path, str],
        ontology_data_model_path: Union[Path, str],
        db_ids_file_path: Union[Path, str],
    ):
        config.DATABASE_URL = neo4j_bolt_url
        self.ontology = Neo4jOntologyConfig(
            ontology_kinds_hierarchy_path, ontology_data_model_path
        )
        self.db_ids_file_path = Path(db_ids_file_path)

    def _is_identical_id(self, id_: str) -> bool:
        """Checks if the given id is in the database or not."""
        ids = []
        if self.db_ids_file_path.exists():
            with open(self.db_ids_file_path, "r+", encoding="utf-8") as file:
                for line in file:
                    ids.append(line.strip())
        else:
            open(self.db_ids_file_path, "w", encoding="utf-8").close()
        if id_ in ids:
            return False
        else:
            return True

    def _store_id(self, id_):
        """Saves the given id to a db_ids file."""
        with open(self.db_ids_file_path, "a", encoding="utf-8") as file:
            file.write(id_ + "\n")

    def _create_new_state(
        self, id_: str, create_date: Optional[datetime.datetime] = None
    ):
        """Creates a new State node for an entity with the exact same
        properties and relationships as the previous one

        Args:
          id_: id of entity, for which we're creating new state
          create_date: New state creation date

        """
        if create_date is None:
            create_date = datetime.datetime.now()
        match_a, filter_a = querymaker.match_node_query(
            "a", properties_filter={"Id": id_}
        )
        set_query, _ = querymaker.patch_property_query(
            "a", updates={}, change_date=create_date
        )
        return_query = querymaker.return_nodes_or_relationships_query(["node"])
        query = "\n".join([match_a, set_query, return_query])
        db.cypher_query(query, filter_a)

    def _get_current_state_node(self, id_: str) -> Optional[neo4j_graph.Node]:
        """Retrieves the current State node: by a given Entity node.

        Args:
          id_ : Entity id

        Returns:
          The state node that has "CURRENT" relationship with entity

        """
        match_query, params = querymaker.match_node_query(
            "s", properties_filter={"Id": id_}
        )
        get_query = querymaker.get_current_state_query("s")
        return_query = querymaker.return_nodes_or_relationships_query(["node"])

        query = "\n".join([match_query, get_query, return_query])
        try:
            node, _ = db.cypher_query(query, params)
            if node:
                [[node]] = node
                return node
            else:
                return None
        except ClientError as exc:
            logging.error(
                """The given entity has no current state node. Either the entity is no longer active,
                or it's not a versioner node. Try calling get_entity_state_by_date
                The next error has occured %s""",
                exc,
            )
            return None

    def _get_entity_nodes(
        self, list_of_ids: List[str]
    ) -> Optional[List[neo4j_graph.Node]]:
        """Looks up for and return entities with given ids.

        Args:
          list_of_ids: list of entities ids

        Returns:
          List of entity nodes.

        """
        match_query, _ = querymaker.match_node_query("a")
        where_query = querymaker.where_property_value_in_list_query(
            "a", "Id", list_of_ids
        )
        return_query = querymaker.return_nodes_or_relationships_query(["a"])

        query = "\n".join([match_query, where_query, return_query])

        nodes, _ = db.cypher_query(query)
        if nodes:
            return [node[0] for node in nodes]
        else:
            return None

    def _is_valid_relationship(
        self,
        id_a,
        relationship_kind,
        id_b,
        rel_property_kinds,
        rel_property_values,
    ):
        """Checks if a relationship between two entities is valid according to the data model."""
        if (a_node := self._get_entity_nodes([id_a])) is not None:
            kind_a = next(iter(a_node[0].labels))
        else:
            logging.error(
                """Id '%s' is not defined, in DB, relationship (%s,%s,%s) has not been created""",
                id_a,
                id_a,
                relationship_kind,
                id_b,
            )
            return False
        if (b_node := self._get_entity_nodes([id_b])) is not None:
            kind_b = next(iter(b_node[0].labels))
        else:
            logging.error(
                """Id '%s' is not defined in DB, relationship (%s,%s,%s) has not been created""",
                id_b,
                id_a,
                relationship_kind,
                id_b,
            )
            return False

        if not self.ontology._is_valid_relationship_model(
            kind_a, relationship_kind, kind_b, rel_property_kinds, rel_property_values
        ):
            relationship_model = (kind_a, relationship_kind, kind_b)
            logging.error(
                """Relationship "(%s, %s, %s)" couldn't be created. "%s" is not a valid """
                """relationship in ontology data model""",
                id_a,
                relationship_kind,
                id_b,
                relationship_model,
            )
            return False
        return True

    def drop_database(
        self,
    ):
        """Clears database."""
        clear_neo4j_database(db)

        ontology_kinds_hierarchy = self.ontology.ontology_kinds_hierarchy_path
        if os.path.exists(ontology_kinds_hierarchy):
            os.remove(ontology_kinds_hierarchy)

        data_model_file = self.ontology.ontology_data_model_path
        if os.path.exists(data_model_file):
            os.remove(data_model_file)

        db_ids = self.db_ids_file_path
        if os.path.exists(db_ids):
            os.remove(db_ids)

    def create_entity(
        self,
        kind: str,
        entity_id: str,
        property_kinds: List[str],
        property_values: list,
        create_date: Optional[datetime.datetime] = None,
    ):
        """Creates new entity.

        Args:
          kind: entity kind
          entity_id: Entity id
          property_kinds: Entity properties
          property_values: Entity property values
          create_date: entity creation date

        Returns:
          created entity in case of success, None otherwise
        """
        if len(property_kinds) != len(property_values):
            logging.error(
                "Number of property kinds doesn't correspond properly with number of property "
                "values. Should be equal"
            )
            return None
        if create_date is None:
            create_date = datetime.datetime.now()
        if not self._is_identical_id(entity_id):
            logging.error("The same id exists in database")
            return None
        property_kinds.append("_deleted")
        property_values.append(False)

        if not self.ontology._are_valid_entity_kind_properties(
            property_kinds,
            property_values,
            entity_kind=kind,
        ):
            return None
        immutable_properties = {"Id": entity_id}
        query, params = querymaker.init_entity_query(
            kind,
            immutable_properties,
            dict(zip(property_kinds, property_values)),
            create_date,
        )
        return_query = querymaker.return_nodes_or_relationships_query(["node"])
        query = "\n".join([query, return_query])

        nodes, _ = db.cypher_query(query, params)
        self._store_id(entity_id)

        if nodes:
            [[entity]] = nodes
            return entity
        else:
            return None

    def delete_entity(
        self,
        entity_id: str,
        deletion_date: Optional[datetime.datetime] = None,
    ):
        """Makes an entity a thing of the past by marking it as deleted using the _deleted property.

        Args:
          entity_id: entity id
          deletion_date: the date of entity deletion

        Returns:
          In case of error: None.
          In case of success: State node

        """
        if deletion_date is None:
            deletion_date = datetime.datetime.now()
        if not self._get_entity_nodes([entity_id]):
            logging.error("No such a node to be deleted")
            return None

        return self.create_or_update_property_of_entity(
            entity_id, "_deleted", True, deletion_date
        )

    def create_or_update_properties_of_entities(
        self,
        entity_ids: List[str],
        property_kinds: List[str],
        new_property_values: List[Any],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates and Adds a batch of properties of a batch of entities.

        Args:
          entity_ids: entities ids
          property_kinds: properties kinds to be updated or added
          new_property_values: properties values that correspond respectively to property_kinds
          change_date: the date of entities updating

        Returns:
          State nodes in case of success or None in case of error.
        """
        if len(property_kinds) != len(new_property_values):
            logging.error(
                "Number of property kinds don't correspont properly with number of property "
                "values. Should be equal"
            )
            return None

        entities = self._get_entity_nodes(entity_ids)
        if entities:
            for entity in entities:
                kinds_frozenset = entity.labels
                entity_kind = next(iter(kinds_frozenset))
                if not self.ontology._are_valid_entity_kind_properties(
                    property_kinds,
                    new_property_values,
                    entity_kind,
                ):
                    return None

        for id_ in entity_ids:
            entity = self._get_entity_nodes([id_])
            if not entity:
                logging.error(
                    "Node with Id %s is not in database\nNothing has been updated", id_
                )
                return None
        if change_date is None:
            change_date = datetime.datetime.now()
        updates = dict(zip(property_kinds, new_property_values))

        match_a, _ = querymaker.match_node_query("a")
        where_a = querymaker.where_property_value_in_list_query("a", "Id", entity_ids)
        with_a = querymaker.with_query(["a"])
        set_query, updated_updates = querymaker.patch_property_query(
            "a", updates, change_date
        )
        return_query = querymaker.return_nodes_or_relationships_query(["node"])

        params = {**updated_updates}
        query = "\n".join([match_a, where_a, with_a, set_query, return_query])

        nodes, _ = db.cypher_query(query, params)

        if nodes:
            return nodes
        else:
            logging.warning("No node has been updated")
            return None
    
    def create_or_update_properties_of_entity(
        self,
        entity_id: str,
        property_kinds: List[str],
        new_property_values: List[Any],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates and Adds a batch of properties of a single entity.

        Args:
          entity_id: entity id
          property_kinds: properties kinds to be updated or added
          new_property_values: properties values that correspont respectively to property_kinds
          change_date: the date of entity updating

        Returns:
          State node in case of success or None in case of error.

        """
        nodes = self.create_or_update_properties_of_entities(
            [entity_id], property_kinds, new_property_values, change_date
        )
        if nodes:
            [[node]] = nodes
            return node
        else:
            return None
    
    def create_or_update_property_of_entity(
        self,
        entity_kind: str,
        property_kind: str,
        property_value: Any,
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates a single property of a given entity.

        Args:
          entity_kind: entity id
          property_kind: kind of the property
          property_value: value of the property
          change_date: the date of entity updating

        Returns:
          State node in case of success or None in case of error.

        """
        nodes = self.create_or_update_properties_of_entities(
            [entity_kind], [property_kind], [property_value], change_date
        )
        if nodes:
            [[node]] = nodes
            return node
        else:
            return None

    def delete_properties_from_entities(self, entity_ids: List[str], property_kinds: List[str]):
        raise NotImplementedError

    def delete_properties_from_entity(
        self,
        entity_id: str,
        property_kinds: List[str],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Removes a batch of properties from a given entity.

        Args:
           entity_id: entity id
           property_kinds: property keys to be removed
           change_date: the date of node updating

        Returns:
          State node in case of success or None in case of error.
        """
        current_state = self._get_current_state_node(entity_id)
        if current_state is None:
            logging.warning(
                "No property was removed. No entity with specified id was found"
            )
            return None
        if change_date is None:
            change_date = datetime.datetime.now()

        self._create_new_state(entity_id, change_date)
        new_current_state = self._get_current_state_node(entity_id)
        if new_current_state is None:
            return None

        match_state, id_updated = querymaker.match_node_query("state", "State")
        where_state = querymaker.where_internal_id_equal_to(
            ["state"], [new_current_state.id]
        )
        remove_state = querymaker.remove_properties_query("state", property_kinds)
        return_state = querymaker.return_nodes_or_relationships_query(["state"])

        query = "\n".join([match_state, where_state, remove_state, return_state])

        [[node]], _ = db.cypher_query(query, id_updated)
        return node

    def delete_property_from_entity(self, entity_id: str, property_kind: str):
        """Removes a property from a given entity.

        Args:
           entity_id: entity id
           property_kind: property kind to be removed

        Returns:
          State node in case of success or None in case of error.
        """
        return self.delete_properties_from_entity(entity_id, [property_kind])

    def get_all_entities(self):
        """Returns all entities in the database with their properties."""
        all_entity_ids = []
        properties_of_all_entities = []
        if self.db_ids_file_path.exists():
            with open(self.db_ids_file_path, "r", encoding="utf-8") as file:
                for line in file:
                    all_entity_ids.append(line.strip())
        for entity_id in all_entity_ids:
            entity_props = {"Id": entity_id}
            entity_props.update(self.get_properties_of_entity(entity_id))
            properties_of_all_entities.append(entity_props)
        return properties_of_all_entities

    def get_properties_of_entity(self, entity_id: str):
        """Returns properties of an entity"""
        return dict(self._get_current_state_node(entity_id).items())

    def create_relationship(
        self,
        id_a: str,
        relationship_kind: str,
        id_b: str,
        rel_property_kinds: Optional[List[str]] = None,
        rel_property_values: Optional[List[Any]] = None,
        create_date: Optional[datetime.datetime] = None,
    ) -> Optional[neo4j_graph.Relationship]:
        """Finds entities A and B and set a relationship between them.

        Direction is from entity A to entity B.

        Args:
          id_a: id of entity A
          relationship_kind: relationship between entities A and B
          id_b: id of entity B
          rel_property_kinds: Relationship properties
          rel_property_values: Relationship property values
          create_date: relationship creation date

        Returns:
            neo4j relationship object in case of success or None in case of error

        """
        if rel_property_kinds is None:
            rel_property_kinds = []
        if rel_property_values is None:
            rel_property_values = []

        if not self._is_valid_relationship(
            id_a, relationship_kind, id_b, rel_property_kinds, rel_property_values
        ):
            return None

        rel_property_kinds.append("_deleted")
        rel_property_values.append(False)

        if create_date is None:
            create_date = datetime.datetime.now()
        match_a, filter_a = querymaker.match_node_query(
            "a", properties_filter={"Id": id_a}
        )
        match_b, filter_b = querymaker.match_node_query(
            "b", properties_filter={"Id": id_b}
        )
        rel_properties = dict(zip(rel_property_kinds, rel_property_values))
        rel, rel_properties = querymaker.create_relationship_query(
            "a", relationship_kind, rel_properties, "b", create_date
        )
        with_query = querymaker.with_query(["a", "b"])
        query = "\n".join([match_a, match_b, with_query, rel])
        params = {**filter_a, **filter_b, **rel_properties}

        try:
            relationship, _ = db.cypher_query(query, params)
            [[relationship]] = relationship
        except ClientError as exc:
            logging.error(
                "No new relationship has been created.\nRaised error: %r\n"
                "It could be because the relationship you're trying to create is already in database",
                exc,
            )
            return None
        return relationship

    def search_for_relationships(
        self,
        relationship_kind: Optional[str] = None,
        id_a: str = "",
        id_b: str = "",
        rel_properties_filter: Optional[dict] = None,
        kind_a: str = "",
        kind_b: str = "",
        limit=10,
        return_query_instead_of_relationships: bool = False,
        search_all_states=False,
    ) -> Union[
        List[List[Union[neo4j_graph.Node, neo4j_graph.Relationship]]], Tuple[str, dict]
    ]:
        """Searches existing relationships.

        Args:
          relationship_kind: relationship type
          id_a: id of entity A
          id_b: id of entity B
          rel_properties_filter: relationship keyword properties for matching
          kind_a: kind of entity A
          kind_b: kind of entity B
          limit: maximum number of relationships to be returned
          return_query_instead_of_relationships: False for returning the found relationship.
                      True for returning (query, params) of that relationship matching.

        Returns:
          neo4j relationships list

        """
        if rel_properties_filter is None:
            rel_properties_filter = {}
        if relationship_kind is None:
            relationship_kind = ""

        a_properties_filter = {}
        b_properties_filter = {}
        if id_a:
            a_properties_filter = {"Id": id_a}
        if id_b:
            b_properties_filter = {"Id": id_b}
        state_relationship_kind = "HAS_STATE" if search_all_states else "CURRENT"
        match_a, filter_a = querymaker.match_node_query(
            "a", kind=kind_a, properties_filter=a_properties_filter
        )
        match_b, filter_b = querymaker.match_node_query(
            "b", kind=kind_b, properties_filter=b_properties_filter
        )
        (
            rel_query,
            rel_properties_filter,
        ) = querymaker.match_relationship_versioner_query(
            "a",
            "r",
            relationship_kind,
            rel_properties_filter,
            "b",
            state_relationship_kind,
        )

        return_query = querymaker.return_nodes_or_relationships_query(
            ["a", state_relationship_kind.lower(), "state", "r", "b"]
        )
        limit_query = querymaker.limit_query(limit)

        query = "\n".join([match_a, match_b, rel_query, return_query, limit_query])
        params = {**filter_a, **filter_b, **rel_properties_filter}

        if return_query_instead_of_relationships:
            query = "\n".join([match_a, match_b, rel_query])
            return query, params

        rels, _ = db.cypher_query(query, params)
        return rels

    # Extra
    def create_or_update_properties_of_relationship(
        self,
        id_a: str,
        relationship_kind: str,
        id_b: str,
        updated_property_kinds: List[str],
        updated_property_values: List[Any],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Creates or updates a relationship properties.

        Args:
          id_a: id of entity A
          relationship_kind: relationship type
          id_b: id of entity B
          updated_property_kinds: Entity properties to be updated
          updated_property_values: New entity property values
          change_date: the date of node updating

        Returns:

        """
        if change_date is None:
            change_date = datetime.datetime.now()
        if updated_property_kinds is None:
            updated_property_kinds = []
        if updated_property_values is None:
            updated_property_values = []

        if not self._is_valid_relationship(
            id_a,
            relationship_kind,
            id_b,
            updated_property_kinds,
            updated_property_values,
        ):
            return None

        self._create_new_state(id_a, change_date)

        # update relationship of the new state
        match_a, filter_a = querymaker.match_node_query(
            "a", properties_filter={"Id": id_a}
        )
        match_b, filter_b = querymaker.match_node_query(
            "b", properties_filter={"Id": id_b}
        )
        rel_match, filter_r = querymaker.match_relationship_versioner_query(
            "a", "r", relationship_kind, {}, "b", state_relationship_kind="CURRENT"
        )
        updates = dict(zip(updated_property_kinds, updated_property_values))
        set_query, updated_updates = querymaker.set_property_query("r", updates)

        params = {**filter_a, **filter_b, **filter_r, **updated_updates}
        query = "\n".join([match_a, match_b, rel_match, set_query])

        return db.cypher_query(query, params)

    def delete_relationship(
        self,
        id_a: str,
        relationship_kind: str,
        id_b: str,
        deletion_date: Optional[datetime.datetime] = None,
    ):
        """Removes a relationship between two entities A and B using the versioner.

        A new state node will be created via the versioner to indicate a new state
        without the deleted relationship.

        Args:
          relationship_kind: relationship type
          id_a: id of entity A
          id_b: id of entity B
          deletion_date: the date of relationship deletion

        Returns:
          True if the relationship
        """
        if deletion_date is None:
            deletion_date = datetime.datetime.now()
        if not self.search_for_relationships(relationship_kind, id_a=id_a, id_b=id_b):
            logging.error("No such a relationship to be deleted")
            return None

        match_a, match_b = [""] * 2
        match_a, filter_a = querymaker.match_node_query(
            "a", properties_filter={"Id": id_a}
        )
        match_b, filter_b = querymaker.match_node_query(
            "b", properties_filter={"Id": id_b}
        )

        delete_query = querymaker.delete_relationship_versioner_query(
            "a", relationship_kind, "b", deletion_date
        )
        return_query = querymaker.return_nodes_or_relationships_query(["result"])

        query = "\n".join([match_a, match_b, delete_query, return_query])
        params = {**filter_a, **filter_b}

        result, _ = db.cypher_query(query, params)
        return result

    def get_entities_by_date(self, entity_ids: List[str], date_to_inspect: datetime.datetime):
        """Returns a batch of entities properties, which were valid on the inspected date.

        Args:
          list_of_ids: Entity ids
          date_to_inspect: Date, on which the state is required.
                           Should be of format: "%Y-%m-%dT%H:%M:%S"

        returns:
          State nodes in case of success or None in case of error.
        """
        match_a, node_properties_filter = querymaker.match_node_query("a")
        where_id = querymaker.where_property_value_in_list_query("a", "Id", entity_ids)
        match_r, rel_properties_filter = querymaker.match_relationship_cypher_query(
            var_name_a="a",
            var_name_r="has_state",
            relationship_kind="HAS_STATE",
            rel_properties_filter={},
            var_name_b="state",
        )
        date_to_inspect = date_to_inspect.strftime("%Y-%m-%dT%H:%M:%S")
        where_on_date = querymaker.where_state_on_date(date_to_inspect)

        return_query = querymaker.return_nodes_or_relationships_query(["state"])

        query = "\n".join([match_a, where_id, match_r, where_on_date, return_query])
        params = {**node_properties_filter, **rel_properties_filter}

        state_nodes, _ = db.cypher_query(query, params)

        if state_nodes:
            return [dict(node[0].items()) for node in state_nodes]
        else:
            return None

    def get_entity_by_date(self, entity_id: str, date_to_inspect: datetime.datetime):
        """Returns an entity properties, which were valid on the inspected date."""
        entities = self.get_entities_by_date([entity_id], date_to_inspect)
        if entities:
            return entities[0]

class TerminusdbKnowledgeGraph(KnowledgeGraph):
    def __init__(
        self,
        team: str,
        db_name: str,
        local: bool = False,
        username: str = "admin",
        password: str = "root",
    ):
        self._team = team
        self._db = db_name
        if local:
            self._client = WOQLClient("http://localhost:6363", account=username, team=self._team, key=password)
            try:
                self._client.connect(team=self._team, db=self._db)
            except InterfaceError:
                self._client.connect(team=self._team)
                self._client.create_database(db_name)
        else:
            endpoint = f"https://cloud.terminusdb.com/{self._team}/"
            self._client   = WOQLClient(endpoint)
            try:
                self._client.connect(team=self._team, use_token=True, db=self._db)
            except InterfaceError:
                self._client.connect(team=self._team, use_token=True)
                self._client.create_database(db_name)
        self.ontology = TerminusdbOntologyConfig(self._client)

    def drop_database(self):
        DB = self._client.db
        TEAM = self._client.team
        self._client.delete_database(DB, team=TEAM)
        self._client.create_database(DB, team=TEAM)
        logging.info("Database was recreated successfully")

    def create_entity(self, kind: str, entity_id: str, property_kinds: List[str], property_values: list):
        """create an entity, or rewrite above it if it exists"""
        return self._client.insert_document({
            "@type": kind,
            "@id": entity_id,
            **dict(zip(property_kinds, property_values)),
        })

    def delete_entity(self, entity_id: str):
        return self._client.delete_document({
            "@id": entity_id,
        })

    def create_or_update_properties_of_entities(self, entity_ids: List[str], property_kinds: List[str], new_property_values: List[Any]):
        for entity_id in entity_ids:
            self.create_or_update_properties_of_entity(
                entity_id, property_kinds, new_property_values
            )
    
    def create_or_update_properties_of_entity(self, entity_id: str, property_kinds: List[str], new_property_values: List[Any]):
        entity_kind = self.get_properties_of_entity(entity_id)["@type"]
        entity_kind_properties = self.ontology.get_entity_kind(entity_kind)
        for prop_kind in property_kinds:
            if prop_kind not in entity_kind_properties:
                raise ValueError(f"Property {prop_kind} should be in the ontology before adding it to graph")
        entity = self.get_properties_of_entity(entity_id)

        for idx, prop_kind in enumerate(property_kinds.copy()):
            entity_props = self.ontology.get_entity_kind(entity["@type"])
            if prop_kind not in entity_props:
                continue
            # if it's a relationship
            if (not entity_props.get(prop_kind)["@class"].startswith("xsd")
               and prop_kind in entity):
                entity[prop_kind].append(new_property_values[idx])
                property_kinds.pop(idx)
                new_property_values.pop(idx)

        entity.update({
                **dict(zip(property_kinds, new_property_values)),
            })
        try:
            return self._client.update_document(entity)
        except DatabaseError as e:
            raise DatabaseError(
                f"""You must supply the required properties of this document at first. You can fill them with empty values suin.
                Error: {e.error_obj["api:error"]["api:witnesses"][0]["@type"]}
                Field: {e.error_obj["api:error"]["api:witnesses"][0]["field"]}
                """
            )
    
    def create_or_update_property_of_entity(self, entity_id: str, property_kind: str, new_property_value: Any):
        return self.create_or_update_properties_of_entity(entity_id, [property_kind], [new_property_value])

    def delete_properties_from_entities(self, entity_ids: List[str], property_kinds: List[str]):
        for entity_id in entity_ids:
            self.delete_properties_from_entity(entity_id, property_kinds)
    
    def delete_properties_from_entity(self, entity_id: str, property_kinds: List[str]):
        none_value_list = [None] * len(property_kinds)
        return self.create_or_update_properties_of_entity(
            entity_id, property_kinds, none_value_list
        )

    def delete_property_from_entity(self, entity_id: str, property_kind: str):
        return self.delete_properties_from_entity(entity_id, [property_kind])

    def get_all_entities(self):
        return self._client.get_all_documents(as_list=True)

    def get_properties_of_entity(self, entity_id: str):
        return self._client.get_document(entity_id)

    def create_relationship(self, id_a: str, relationship_kind: str, id_b: str):
        self.create_or_update_property_of_entity(id_a, relationship_kind, id_b)

    # TODO: you can discuss the case of relationship_kind is None to search for rels between a and b, rels from a, or rels to b
    def search_for_relationships(
        self,
        relationship_kind: str,
        id_a: Optional[str] = None,
        id_b: Optional[str] = None,
    )-> List[Tuple[str, str]]:
        """Search relationships OR PROPERTIES depending on the filters given."""
        relationship_tuples = []
        if id_a is not None:
            props_a = self.get_properties_of_entity(id_a)
            if relationship_kind in props_a:
                relationship_tuples.append((id_a, props_a[relationship_kind]))
        else:
            for entity in self.get_all_entities():
                if relationship_kind in entity:
                    if id_b is not None:
                        if entity[relationship_kind]!=id_b:
                            continue
                    relationship_tuples.append((entity["@id"], entity[relationship_kind]))
        return relationship_tuples

    # Extra
    def update_relationship(self, id_a: str, relationship_kind: str, new_id_b: Optional[str] = None):
        return self.create_or_update_property_of_entity(id_a, relationship_kind, new_id_b)

    def delete_relationship(self, id_a: str, relationship_kind: str, id_b: Optional[str] = None):
        return self.create_or_update_property_of_entity(id_a, relationship_kind, None)

    def get_entities_by_date(self, entity_ids: List[str], date_to_inspect: datetime.datetime):
        history = self._client.get_commit_history()
        commits = [commit for commit in history if commit["timestamp"]<=date_to_inspect]
        if not commits:
            raise ValueError("At provided timestamp no commit has been committed to database yet")
        commit_id = commits[0]["identifier"]
        path = f"{self._team}/{self._db}/local/commit/{commit_id}"
        queries = [
                WOQL().using(
                    path,
                    WOQL().read_object(f"terminusdb:///data/{entity_id}", f"v:{entity_id}")
                ) for entity_id in entity_ids
        ]
        query_result = self._client.query(WOQL().woql_and(*queries))
        return query_result["bindings"]

    def get_entity_by_date(self, entity_id: str, date_to_inspect: datetime.datetime):
        entities = self.get_entities_by_date([entity_id], date_to_inspect)
        if entities:
            return entities[0][entity_id]