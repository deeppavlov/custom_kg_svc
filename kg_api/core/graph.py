import os
from typing import Optional, List
import logging
import datetime
from neomodel import db, config, clear_neo4j_database
import neo4j
from kg_api.utils.settings import OntologySettings
import kg_api.core.querymaker as querymaker
import kg_api.core.ontology as ontology


def drop_database():
    """Clears database."""
    clear_neo4j_database(db)

    ontology_file = "kg_api/database/ontology_graph.pickle"
    if os.path.exists(ontology_file):
        os.remove(ontology_file)

    db_ids = "kg_api/database/db_ids.txt"
    if os.path.exists(db_ids):
        os.remove(db_ids)


def is_identical_id(id_: str) -> bool:
    """Checks if the given id is in the database or not."""
    ids = []
    ids_file = "kg_api/database/db_ids.txt"
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
    with open("kg_api/database/db_ids.txt", "a", encoding="utf-8") as file:
        file.write(id_ +"\n")


def create_entity(
    kind: str,
    id_: str,
    state_properties: dict,
    create_date: Optional[datetime.datetime] = None,
    parent_kind: str = "Kind"
):
    """Creates new entity.

    Args:
      kind: entity kind
      id_: Entity id
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: entity creation date
      parent_kind: parent of kind. e.g. Dog -> Animal

    Returns:
      created entity in case of success, None otherwise
    """
    if create_date is None:
        create_date = datetime.datetime.now()
    if not is_identical_id(id_):
        logging.error("The same id exists in database")
        return None
    immutable_properties = {"Id": id_}
    query, params = querymaker.init_entity_query(
        kind, immutable_properties, state_properties, create_date
    )
    return_ = querymaker.return_nodes_or_relationships_query(["node"])
    query = "\n".join([query, return_])

    nodes, _ = db.cypher_query(query, params)
    store_id(id_)
    ontology.create_kind(kind, parent_kind)

    if nodes:
        [[entity]] = nodes
        return entity
    else:
        return None


def get_entity_by_id(id_: str) -> Optional[neo4j.graph.Node]: # type: ignore
    """Looks up for and return entity with given id.

    Args:
      id_: entity id

    Returns:
      Entity node.

    """
    list_of_ids = [id_]
    entities = get_entities_by_id(list_of_ids)
    if entities:
        [[entity]] = entities
    else:
        entity = None
    return entity


# Needed for batch operations.
def get_entities_by_id(list_of_ids: list) -> Optional[List[neo4j.graph.Node]]: # type: ignore
    """Looks up for and return entities with given ids.

    Args:
      list_of_ids: list of entities ids

    Returns:
      List of entity nodes.

    """
    match_query, _ = querymaker.match_node_query("a")
    where_query = querymaker.where_property_value_in_list_query("a", "Id", list_of_ids)
    return_query = querymaker.return_nodes_or_relationships_query(["a"])

    query = "\n".join([match_query, where_query, return_query])

    nodes, _ = db.cypher_query(query)
    if nodes:
        return nodes
    else:
        return None


# it would be useful to add support for hierarchical ontological search, e.g.: user can specify "Kind" as 
# "DP_Kinds_Kind" (which is a super generic thing), and then we should return all nodes whose "Kind" is 
# either set as "DP_Kinds_Kind" or is its child (direct or indirect), and that should be a parameter
# e.g., "filter_by_children_kinds". By default it could be set to False.
def search_for_entities(kind: str = "", properties_filter: Optional[dict] = None, limit=10) -> list:
    """Searches existing entities.

    Args:
      kind: entity kind
      properties_filter: entity keyword properties for matching
      limit: maximum number of returned nodes

    Returns:
      Entity nodes list

    """
    if properties_filter is None:
        properties_filter = {}

    descendant_kinds = ontology.get_descendant_kinds(kind)
    descendant_kinds.append(kind)

    match_a, filter_a = querymaker.match_node_query("a", properties_filter=properties_filter)
    where_ = querymaker.where_entity_kind_in_list_query("a", descendant_kinds)
    return_a = querymaker.return_nodes_or_relationships_query(["a"])
    limit_a = querymaker.limit_query(limit)
    query = "\n".join([match_a, where_, return_a, limit_a])

    nodes, _ = db.cypher_query(query, filter_a)
    return nodes


def create_or_update_property_of_entity(
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
    nodes = create_or_update_properties_of_entities(
        [id_], [property_kind], [property_value], change_date
    )
    if nodes:
        [[node]] = nodes
        return node
    else:
        return None


def create_or_update_properties_of_entities(
    list_of_ids: list,
    list_of_property_kinds:list,
    list_of_property_values:list,
    change_date: Optional[datetime.datetime] = None,
):
    """Updates and Adds properties of entities for batch operations.

    Args:
      list_of_ids: entities ids
      list_of_property_kinds: properties kinds to be updated or added
      list_of_property_values: properties values that correspont respectively to property_kinds
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

    match_a, _ = querymaker.match_node_query("a")
    where_a = querymaker.where_property_value_in_list_query("a", "Id", list_of_ids)
    with_a = querymaker.with_query(["a"])
    set_query, updated_updates = querymaker.patch_property_query(
        "a", updates, change_date
    )
    return_ = querymaker.return_nodes_or_relationships_query(["node"])

    params = {**updated_updates}
    query = "\n".join([match_a, where_a, with_a, set_query, return_])

    nodes, _ = db.cypher_query(query, params)

    if nodes:
        return nodes
    else:
        logging.warning("No node has been updated")
        return None


def create_or_update_properties_of_entity(
    id_: str,
    list_of_property_kinds:list,
    list_of_property_values:list,
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
    nodes = create_or_update_properties_of_entities(
        [id_], list_of_property_kinds, list_of_property_values, change_date
    )
    if nodes:
        [[node]] = nodes
        return node
    else:
        return None


def remove_property_from_entity(
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

    property_kinds_list = []
    property_kinds_list.append(property_kind) 

    return delete_properties_from_entity(id_, property_kinds_list, change_date)


def delete_properties_from_entity(
        id_: str,
        property_kinds: list,
        change_date: Optional[datetime.datetime] = None,
    ):
    """Deletes a property from a given entity.

    Args:
       id_: entity id
       property_kinds: property keys to be deleted
       change_date: the date of node updating

    Returns:
      State node in case of success or None in case of error.
    """
    current_state = get_current_state(id_)
    if not current_state:
        logging.warning("No property was removed. No entity with specified id was found")
        return

    property_values = [""]*len(property_kinds)
    create_or_update_properties_of_entity(id_, property_kinds, property_values, change_date)

    new_current_state = get_current_state(id_)

    match_state, id_updated = querymaker.match_node_query("state", "State")
    where_state = querymaker.where_node_internal_id_equal_to("state", new_current_state.id)
    remove_state = querymaker.remove_properties_query("state", property_kinds)
    return_state = querymaker.return_nodes_or_relationships_query(["state"])

    query = "\n".join([match_state, where_state, remove_state, return_state])

    [[node]], _ = db.cypher_query(query, id_updated)
    return node


def delete_entity(
    id_: str,
    completely=False,
    deletion_date: Optional[datetime.datetime] = None,
):
    """Deletes an entity completely from database or make it a thing of the past.

    Args:
      id_: entity id
      completely: if True, then the entity will be deleted completely from DB
                  with all its relationships. If False, the entity will be marked
                  as deleted by the _deleted property.
      deletion_date: the date of entity deletion

    Returns:
      In case of error: None.
      In case of success: if completely: returns True, of State node otherwise

    """
    if deletion_date is None:
        deletion_date = datetime.datetime.now()
    properties_filter = {"Id": id_}
    if not search_for_entities(properties_filter=properties_filter):
        logging.error("No such a node to be deleted")
        return None
    if completely:
        match_a, filter_a = querymaker.match_node_query("a", properties_filter=properties_filter)
        delete_a = querymaker.delete_query("a")

        query = "\n".join([match_a, delete_a])
        params = filter_a

        db.cypher_query(query, params)
        return True
    else:
        return create_or_update_property_of_entity(
            properties_filter["Id"], "_deleted", True, deletion_date
        )


def create_relationship(
    id_a: str,
    relationship_kind: str,
    rel_properties: dict,
    id_b: str,
    create_date: Optional[datetime.datetime] = None,
):
    """Finds entities A and B and set a relationship between them.

    Direction is from entity A to entity B.

    Args:
      id_a: id of entity A
      relationship_kind: relationship between entities A and B
      rel_properties: relationship properties
      id_b: id of entity B
      create_date: relationship creation date

    Returns:

    """
    if create_date is None:
        create_date = datetime.datetime.now()
    match_a, filter_a = querymaker.match_node_query("a", properties_filter={"Id":id_a})
    match_b, filter_b = querymaker.match_node_query("b", properties_filter={"Id":id_b})
    rel, rel_properties = querymaker.create_relationship_query(
        "a", relationship_kind, rel_properties, "b", create_date
    )
    with_ = querymaker.with_query(["a", "b"])
    query = "\n".join([match_a, match_b, with_, rel])
    params = {**filter_a, **filter_b, **rel_properties}

    db.cypher_query(query, params)


def search_relationships(
    relationship_kind: str,
    rel_properties_filter: Optional[dict] = None,
    id_a: str = "",
    id_b: str = "",
    limit=10,
    programmer=0,
) -> list:
    """Searches existing relationships.

    Args:
      relationship_kind: relationship type
      rel_properties_filter: relationship keyword properties for matching
      id_a: id of entity A
      id_b: id of entity B
      limit: maximum number of relationships to be returned
      programmer: False for returning the relationship found. True for returning
                  (query, params) of that relationship matching.

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
    match_a, filter_a = querymaker.match_node_query("a", properties_filter=a_properties_filter)
    match_b, filter_b = querymaker.match_node_query("b", properties_filter=b_properties_filter)
    rel_query, rel_properties_filter = querymaker.match_relationship_versioner_query(
        "a", "r", relationship_kind, rel_properties_filter, "b"
    )

    return_ = querymaker.return_nodes_or_relationships_query(
        ["a", "r", "b"]
    )
    limit_ = querymaker.limit_query(limit)

    query = "\n".join([match_a, match_b, rel_query, return_, limit_])
    params = {**filter_a, **filter_b, **rel_properties_filter}

    if programmer:
        query = "\n".join([match_a, match_b, rel_query])
        return query, params

    rels, _ = db.cypher_query(query, params)
    return rels


def update_relationship(
    relationship_kind: str,
    updates: dict,
    id_a: str,
    id_b: str,
    change_date: Optional[datetime.datetime] = None,
):
    """Updates a relationship properties.

    Args:
      relationship_kind: relationship type
      updates: new properties and updated properties
      id_a: id of entity A
      id_b: id of entity B
      change_date: the date of node updating

    Returns:

    """
    # - It seems that it's not recommended to update relationship properties, as there is no
    #   special function for doing that in the versioner.
    # - Notice that you need to save the relationship's properties before deletion, in order to
    #   use them in creating the new one.

    if change_date is None:
        change_date = datetime.datetime.now()

    delete_relationship(
        relationship_kind, id_a, id_b, deletion_date=change_date
    )
    create_relationship(
        id_a=id_a,
        relationship_kind=relationship_kind,
        rel_properties=updates,
        id_b=id_b,
        create_date=change_date,
    )


def delete_relationship(
    relationship_kind: str,
    id_a: str,
    id_b: str,
    completely: bool = False,
    deletion_date: Optional[datetime.datetime] = None,
) -> Optional[bool]:
    """Deletes a relationship between two entities A and B.

    Args:
      relationship_kind: relationship type
      id_a: id of entity A
      id_b: id of entity B
      completely: if True, then the relationship will be deleted completely
                  from DB. If False, a new state node will be created via the versioner
                  to indicate a new state without the deleted relationship.
      deletion_date: the date of relationship deletion

    Returns:
      True if the relationship
    """
    if deletion_date is None:
        deletion_date = datetime.datetime.now()
    if not search_relationships(relationship_kind, id_a=id_a, id_b=id_b):
        logging.error("No such a relationship to be deleted")
        return None

    if completely:
        match_relationship, params = search_relationships(
            relationship_kind,
            id_a=id_a,
            id_b=id_b,
            programmer=1,
        )
        delete_query = querymaker.delete_query("r", is_node=False)
        query = "\n".join([match_relationship, delete_query])

        db.cypher_query(query, params)
    else:
        match_a, match_b = [""] * 2
        match_a, filter_a = querymaker.match_node_query("a", properties_filter={"Id":id_a})
        match_b, filter_b = querymaker.match_node_query("b", properties_filter={"Id":id_b})

        delete_query = querymaker.delete_relationship_query(
            "a", relationship_kind, "b", deletion_date
        )
        return_ = querymaker.return_nodes_or_relationships_query(["result"])

        query = "\n".join([match_a, match_b, delete_query, return_])
        params = {**filter_a, **filter_b}

        result, _ = db.cypher_query(query, params)
        return result


def get_current_state(id_:str) -> Optional[neo4j.graph.Node]: # type: ignore
    """Retrieves the current State node: by a given Entity node.

    Args:
      id_ : Entity id

    Returns:
      The state node that has "CURRENT" relationship with entity

    """
    match_query, params = querymaker.match_node_query("s", properties_filter={"Id":id_})
    get_query = querymaker.get_current_state_query("s")
    return_query = querymaker.return_nodes_or_relationships_query(["node"])

    query = "\n".join([match_query, get_query, return_query])
    node, _ = db.cypher_query(query, params)
    if node:
        [[node]] = node
        return node
    else:
        logging.error("No current state was found for the given node")
        return None


def get_entities_state_by_date(list_of_ids: list, date_: str):
    """Returns the active state nodes on a given date for many entities.

    Args:
      list_of_ids: Entity ids
      date_: Date, on which the state is required. Should be of format: "%Y-%m-%dT%H:%M:%S"

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
    where_on_date = querymaker.where_state_on_date(date_)

    return_ = querymaker.return_nodes_or_relationships_query(["state"])

    query = "\n".join([match_a, where_id, match_r, where_on_date, return_])
    params = {**node_properties_filter, **rel_properties_filter}

    state_nodes, _ = db.cypher_query(query, params)

    if state_nodes:
        return state_nodes
    else:
        return None


def get_entity_state_by_date(id_: str, date_: str):
    """Returns the active state node on a given date.

    Args:
      id_: Entity id
      date_: Date, on which the state is required. Should be of format: "%Y-%m-%dT%H:%M:%S"

    returns:
      State node in case of success or None in case of error.
    """
    state_nodes = get_entities_state_by_date([id_], date_)
    if state_nodes:
        [[state]] = state_nodes
        return state
    else:
        logging.error("No state nod")
        return None

ontology_settings = OntologySettings()

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
