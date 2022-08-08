import os
from pathlib import Path
from typing import Any, Optional, List, Tuple, Union
import logging
import datetime
from neomodel import db, config, clear_neo4j_database
from neo4j import graph as neo4j_graph
from neo4j.exceptions import ClientError
from deeppavlov_kg.core.ontology import Ontology
from deeppavlov_kg.core import querymaker


class KnowledgeGraph:
    def __init__(
        self,
        neo4j_bolt_url: str,
        ontology_kinds_hierarchy_path: Union[Path, str],
        ontology_data_model_path: Union[Path, str],
        db_ids_file_path: Union[Path, str],
    ):
        config.DATABASE_URL = neo4j_bolt_url

        self.ontology = Ontology(ontology_kinds_hierarchy_path, ontology_data_model_path)
        self.db_ids_file_path = Path(db_ids_file_path)

    @classmethod
    def from_obj(cls, config_obj):
        return cls(
            neo4j_bolt_url=config_obj.neo4j_bolt_url,
            ontology_kinds_hierarchy_path=config_obj.ontology_kinds_hierarchy_path,
            ontology_data_model_path=config_obj.ontology_data_model_path,
            db_ids_file_path=config_obj.db_ids_file_path,
        )

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
        id_: str,
        property_kinds: List[str],
        property_values: List[str],
        create_date: Optional[datetime.datetime] = None,
    ):
        """Creates new entity.

        Args:
          kind: entity kind
          id_: Entity id
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
        if not self._is_identical_id(id_):
            logging.error("The same id exists in database")
            return None
        if not self.ontology.are_valid_entity_kind_properties(
            property_kinds,
            entity_kind=kind,
        ):
            return None
        immutable_properties = {"Id": id_}
        query, params = querymaker.init_entity_query(
            kind,
            immutable_properties,
            dict(zip(property_kinds,property_values)),
            create_date
        )
        return_query = querymaker.return_nodes_or_relationships_query(["node"])
        query = "\n".join([query, return_query])

        nodes, _ = db.cypher_query(query, params)
        self._store_id(id_)

        if nodes:
            [[entity]] = nodes
            return entity
        else:
            return None

    def get_entity_by_id(self, id_: str) -> Optional[neo4j_graph.Node]:
        """Looks up for and return entity with given id.

        Args:
          id_: entity id

        Returns:
          Entity node.

        """
        list_of_ids = [id_]
        entities = self.get_entities_by_id(list_of_ids)
        if entities:
            [[entity]] = entities
        else:
            entity = None
        return entity

    # Needed for batch operations.
    def get_entities_by_id(self, list_of_ids: List[str]) -> Optional[List[neo4j_graph.Node]]:
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
            return nodes
        else:
            return None

    def search_for_entities(
        self,
        kind: str = "",
        properties_filter: Optional[dict] = None,
        filter_by_children_kinds: bool = False,
        limit=10,
    ) -> List[List[neo4j_graph.Node]]:
        """Searches existing entities.

        Args:
          kind: entity kind
          properties_filter: entity keyword properties for matching
          filter_by_children_kinds: False to search for entities of kind 'kind' only.
                                    True to also search for all children kinds of 'kind'.
          limit: maximum number of returned nodes

        Returns:
          Entity nodes list

        """
        if properties_filter is None:
            properties_filter = {}

        kinds_to_search_for = []
        where_query = ""
        if kind:
            if filter_by_children_kinds:
                descendant_kinds = self.ontology.get_descendants_of_entity_kind(kind)
                if descendant_kinds is None:
                    return []
                kinds_to_search_for += descendant_kinds
            kinds_to_search_for.append(kind)
            where_query = querymaker.where_entity_kind_in_list_query(
                "a", kinds_to_search_for
            )

        match_a, filter_a = querymaker.match_node_query("a")
        return_a = querymaker.return_nodes_or_relationships_query(["a"])
        limit_a = querymaker.limit_query(limit)
        query = "\n".join([match_a, where_query, return_a, limit_a])

        nodes, _ = db.cypher_query(query, filter_a)

        if not properties_filter:
            return nodes

        entities = []
        for node in nodes:
            state = self.get_current_state(node[0].get("Id"))
            if state is not None:
                for prop in properties_filter:
                    if state.get(prop) != properties_filter[prop]:
                        continue
                    entities.append(node)

        return entities

    def create_or_update_property_of_entity(
        self,
        id_: str,
        property_kind: str,
        property_value,
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates a single property of a given entity.

        Args:
          id_: entity id
          property_kind: kind of the property
          property_value: value of the property
          change_date: the date of entity updating

        Returns:
          State node in case of success or None in case of error.

        """
        nodes = self.create_or_update_properties_of_entities(
            [id_], [property_kind], [property_value], change_date
        )
        if nodes:
            [[node]] = nodes
            return node
        else:
            return None

    def create_or_update_properties_of_entities(
        self,
        list_of_ids: List[str],
        list_of_property_kinds: List[str],
        list_of_property_values: List[Any],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates and Adds properties of entities for batch operations.

        Args:
          list_of_ids: entities ids
          list_of_property_kinds: properties kinds to be updated or added
          list_of_property_values: properties values that correspond respectively to property_kinds
          change_date: the date of entities updating

        Returns:
          State nodes in case of success or None in case of error.
        """
        if len(list_of_property_kinds) != len(list_of_property_values):
            logging.error(
                "Number of property kinds don't correspont properly with number of property "
                "values. Should be equal"
            )
            return None

        if change_date is None:
            change_date = datetime.datetime.now()
        updates = dict(zip(list_of_property_kinds, list_of_property_values))

        for id_ in list_of_ids:
            entity = self.get_entity_by_id(id_)
            if not entity:
                logging.error(
                    "Node with Id %s is not in database\nNothing has been updated", id_
                )
                return None
            else:
                kinds_frozenset = entity.labels
            kind = next(iter(kinds_frozenset))
            if not self.ontology.are_valid_entity_kind_properties(list_of_property_kinds, kind):
                return None
        if change_date is None:
            change_date = datetime.datetime.now()
        updates = dict(zip(list_of_property_kinds, list_of_property_values))

        match_a, _ = querymaker.match_node_query("a")
        where_a = querymaker.where_property_value_in_list_query("a", "Id", list_of_ids)
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
        id_: str,
        list_of_property_kinds: List[str],
        list_of_property_values: List[Any],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates and Adds entity properties.

        Args:
          id_: entity id
          list_of_property_kinds: properties kinds to be updated or added
          list_of_property_values: properties values that correspont respectively to property_kinds
          change_date: the date of entity updating

        Returns:
          State node in case of success or None in case of error.

        """
        nodes = self.create_or_update_properties_of_entities(
            [id_], list_of_property_kinds, list_of_property_values, change_date
        )
        if nodes:
            [[node]] = nodes
            return node
        else:
            return None

    def remove_property_from_entity(
        self,
        id_: str,
        property_kind: str,
        change_date: Optional[datetime.datetime] = None,
    ):
        """Removes a single property from a given entity.

        Args:
          id_: entity id
          property_kind: kind of the property

        Returns:
          State node in case of success or None in case of error.
        """

        property_kinds_list = [property_kind]

        return self.remove_properties_from_entity(id_, property_kinds_list, change_date)

    def remove_properties_from_entity(
        self,
        id_: str,
        property_kinds: List[str],
        change_date: Optional[datetime.datetime] = None,
    ) -> Optional[neo4j_graph.Node]:
        """Removes a property from a given entity.

        Args:
           id_: entity id
           property_kinds: property keys to be removed
           change_date: the date of node updating

        Returns:
          State node in case of success or None in case of error.
        """
        current_state = self.get_current_state(id_)
        if current_state is None:
            logging.warning(
                "No property was removed. No entity with specified id was found"
            )
            return None
        if change_date is None:
            change_date = datetime.datetime.now()


        self.create_new_state(id_, change_date)
        new_current_state = self.get_current_state(id_)
        if new_current_state is None:
            return None

        match_state, id_updated = querymaker.match_node_query("state", "State")
        where_state = querymaker.where_node_internal_id_equal_to(
            "state", new_current_state.id
        )
        remove_state = querymaker.remove_properties_query("state", property_kinds)
        return_state = querymaker.return_nodes_or_relationships_query(["state"])

        query = "\n".join([match_state, where_state, remove_state, return_state])

        [[node]], _ = db.cypher_query(query, id_updated)
        return node

    def destroy_entity(
        self,
        id_: str,
        deletion_date: Optional[datetime.datetime] = None,
    ):
        """Deletes an entity completely from DB with all its relationships.

        Returns:
          In case of error: None.
          In case of success: State node

        """
        if deletion_date is None:
            deletion_date = datetime.datetime.now()
        if not self.get_entity_by_id(id_):
            logging.error("No such a node to be deleted")
            return None

        match_a, filter_a = querymaker.match_node_query(
            "a", properties_filter={"Id": id_}
        )
        delete_a = querymaker.delete_node_query("a")

        query = "\n".join([match_a, delete_a])
        params = filter_a

        db.cypher_query(query, params)
        return True

    def remove_entity(
        self,
        id_: str,
        deletion_date: Optional[datetime.datetime] = None,
    ):
        """Makes an entity a thing of the past by marking it as deleted using the _deleted property.

        Args:
          id_: entity id
          deletion_date: the date of entity deletion

        Returns:
          In case of error: None.
          In case of success: State node

        """
        if deletion_date is None:
            deletion_date = datetime.datetime.now()
        if not self.get_entity_by_id(id_):
            logging.error("No such a node to be deleted")
            return None

        return self.create_or_update_property_of_entity(id_, "_deleted", True, deletion_date)

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
        if (a_node := self.get_entity_by_id(id_a)) is not None:
            kind_a = next(iter(a_node.labels))
        else:
            logging.error(
                """Id '%s' is not defined self, in DB, relationship (%s,%s,%s) was not created""",
                id_a,
                id_a,
                relationship_kind,
                id_b,
            )
            return None
        if (b_node := self.get_entity_by_id(id_b)) is not None:
            kind_b = next(iter(b_node.labels))
        else:
            logging.error("Id '%s' is not defined self, in DB", id_b)
            return None
        if rel_property_kinds is None:
            rel_property_kinds = []
        if rel_property_values is None:
            rel_property_values = []
        if not self.ontology.is_valid_relationship(
            kind_a, relationship_kind, kind_b, rel_property_kinds
        ):
            relationship_model = (kind_a, relationship_kind, kind_b)
            logging.error(
                """Relationship "(%s, %s, %s)" couldn't be created. "%s" is not a valid relationship """
                """in ontology data model""",
                id_a,
                relationship_kind,
                id_b,
                relationship_model,
            )
            return None

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

    def search_relationships(
        self,
        relationship_kind: str,
        rel_properties_filter: Optional[dict] = None,
        id_a: str = "",
        id_b: str = "",
        kind_a: str = "",
        kind_b: str = "",
        limit=10,
        return_query_instead_of_relationships: bool = False,
        search_all_states=False,
    ) -> Union[
        List[
            List[Union[
                neo4j_graph.Node,
                neo4j_graph.Relationship
            ]]
        ],
        Tuple[str, dict]
    ]:
        """Searches existing relationships.

        Args:
          relationship_kind: relationship type
          rel_properties_filter: relationship keyword properties for matching
          id_a: id of entity A
          id_b: id of entity B
          limit: maximum number of relationships to be returned
          return_query_instead_of_relationships: False for returning the found relationship.
                      True for returning (query, params) of that relationship matching.

        Returns:
          neo4j relationships list

        """
        if rel_properties_filter is None:
            rel_properties_filter = {}

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

    def update_relationship(
        self,
        relationship_kind: str,
        id_a: str,
        id_b: str,
        updated_property_kinds: List[str],
        updated_property_values: List[Any],
        change_date: Optional[datetime.datetime] = None,
    ):
        """Updates a relationship properties.

        Args:
          relationship_kind: relationship type
          id_a: id of entity A
          id_b: id of entity B
          updated_property_kinds: Entity properties to be updated
          updated_property_values: New entity property values
          change_date: the date of node updating

        Returns:

        """
        # - It seems that it's not recommended to update relationship properties, as there is no
        #   special function for doing that in the versioner.
        # - Notice that you need to save the relationship's properties before deletion, in order to
        #   use them in creating the new one.

        if change_date is None:
            change_date = datetime.datetime.now()

        self.create_new_state(id_a, change_date)

        # update relationship of the new state
        match_a, filter_a = querymaker.match_node_query("a", properties_filter={"Id":id_a})
        match_b, filter_b = querymaker.match_node_query("b", properties_filter={"Id":id_b})
        rel_match, filter_r = querymaker.match_relationship_versioner_query(
            "a", "r", relationship_kind, {}, "b", state_relationship_kind="CURRENT"
        )
        updates = dict(zip(updated_property_kinds, updated_property_values))
        set_query, updated_updates = querymaker.set_property_query("r", updates)

        params = {**filter_a, **filter_b, **filter_r, **updated_updates}
        query = "\n".join([match_a, match_b, rel_match, set_query])

        return db.cypher_query(query, params)

    def destroy_relationship(
        self,
        relationship_kind: str,
        id_a: str,
        id_b: str,
        deletion_date: Optional[datetime.datetime] = None,
    ) -> Optional[bool]:
        """Deletes a relationship between two entities A and B completely from DB.

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
        if not self.search_relationships(relationship_kind, id_a=id_a, id_b=id_b):
            logging.error("No such a relationship to be deleted")
            return None

        match_relationship, params = self.search_relationships(
            relationship_kind,
            id_a=id_a,
            id_b=id_b,
            return_query_instead_of_relationships=True,
        )
        delete_query = querymaker.delete_relationship_cypher_query("r")
        query = "\n".join([match_relationship, delete_query])

        db.cypher_query(query, params)

    def remove_relationship(
        self,
        relationship_kind: str,
        id_a: str,
        id_b: str,
        deletion_date: Optional[datetime.datetime] = None,
    ) -> Optional[bool]:
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
        if not self.search_relationships(relationship_kind, id_a=id_a, id_b=id_b):
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

    def get_current_state(self, id_: str) -> Optional[neo4j_graph.Node]:
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
            [[node]] = node
            return node
        except ClientError as exc:
            logging.error(
                """The given entity has no current state node. Either the entity is no longer active,
                or it's not a versioner node. Try calling get_entity_state_by_date
                The next error has occured %s""",
                exc,
            )
            return None

    def create_new_state(self, id_: str, create_date: Optional[datetime.datetime] = None):
        """Creates a new State node for an entity with the exact same
        properties and relationships as the previous one

        Args:
          id_: id of entity, for which we're creating new state
          create_date: New state creation date

        """
        if create_date is None:
            create_date = datetime.datetime.now()
        match_a, filter_a = querymaker.match_node_query("a", properties_filter={"Id":id_})
        set_query, _ = querymaker.patch_property_query(
            "a", updates={}, change_date=create_date
        )
        return_query = querymaker.return_nodes_or_relationships_query(["node"])
        query = "\n".join([match_a, set_query, return_query])
        db.cypher_query(query, filter_a)

    def get_entities_states_by_date(self, list_of_ids: List[str], date_to_inspect: str):
        """Returns the active state nodes on a given date for many entities.

        Args:
          list_of_ids: Entity ids
          date_to_inspect: Date, on which the state is required.
                           Should be of format: "%Y-%m-%dT%H:%M:%S"

        returns:
          State nodes in case of success or None in case of error.
        """
        match_a, node_properties_filter = querymaker.match_node_query("a")
        where_id = querymaker.where_property_value_in_list_query("a", "Id", list_of_ids)
        match_r, rel_properties_filter = querymaker.match_relationship_cypher_query(
            var_name_a="a",
            var_name_r="has_state",
            relationship_kind="HAS_STATE",
            rel_properties_filter={},
            var_name_b="state",
        )
        where_on_date = querymaker.where_state_on_date(date_to_inspect)

        return_query = querymaker.return_nodes_or_relationships_query(["state"])

        query = "\n".join([match_a, where_id, match_r, where_on_date, return_query])
        params = {**node_properties_filter, **rel_properties_filter}

        state_nodes, _ = db.cypher_query(query, params)

        if state_nodes:
            return state_nodes
        else:
            return None

    def get_entity_state_by_date(self, id_: str, date_to_inspect: str):
        """Returns the active state node on a given date.

        Args:
          id_: Entity id
          date_to_inspect: Date, on which the state is required.
                           Should be of format: "%Y-%m-%dT%H:%M:%S"

        returns:
          State node in case of success or None in case of error.
        """
        state_nodes = self.get_entities_states_by_date([id_], date_to_inspect)
        if state_nodes:
            [[state]] = state_nodes
            return state
        else:
            logging.error("No state node")
            return None
