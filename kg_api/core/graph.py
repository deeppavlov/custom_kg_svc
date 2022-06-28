from typing import Optional
import logging
import datetime
from neomodel import db, config, clear_neo4j_database
import neo4j
from kg_api.utils.settings import OntologySettings
import kg_api.core.querymaker as querymaker


def drop_database():
    """Clears database."""
    clear_neo4j_database(db)


def create_node(
    kind: str,
    id_: str,
    state_properties: dict,
    create_date: Optional[datetime.datetime] = None,
):
    """Creates new node.

    Args:
      kind: node kind
      id_: Entity id
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: node creation date

    Returns:

    """
    if create_date is None:
        create_date = datetime.datetime.now()
    immutable_properties = {"Id": id_}
    query, params = querymaker.init_node_query(
        kind, immutable_properties, state_properties, create_date
    )

    db.cypher_query(query, params)


def search_nodes(kind: str = "", properties_filter: Optional[dict] = None, limit=10) -> list:
    """Searches existing nodes.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching
      limit: maximum number of returned nodes

    Returns:
      neo4j nodes list

    """
    if properties_filter is None:
        properties_filter = {}
    match_a, filter_a = querymaker.match_node_query("a", kind, properties_filter)
    return_ = f"RETURN a\n LIMIT {limit}"
    query = "\n".join([match_a, return_])

    nodes, _ = db.cypher_query(query, filter_a)
    return nodes


def create_or_update_property_to_node(
    id_: str,
    property_kind: str,
    property_value,
    change_date: Optional[datetime.datetime] = None,
):
    """Updates a single property of a given node.

    Args:
      id_: entity id
      property_kind: kind of the property
      property_value: value of the property

    Returns:
    Node in case of success or None in case of error.
    """

    updates_dict = {}
    updates_dict[property_kind] = property_value 

    return update_node(id_, updates_dict, change_date)


def update_node(
    id_: str,
    updates: dict,
    change_date: Optional[datetime.datetime] = None,
):
    """Updates and Adds node properties.

    Args:
      id_: entity id
      updates: new properties and updated properties
      change_date: the date of node updating

    Returns:
    Node in case of success or None in case of error. 
    """
    properties_filter = {"Id": id_}
    if change_date is None:
        change_date = datetime.datetime.now()

    nodes = search_nodes(properties_filter=properties_filter)
    if nodes:
        if len(nodes) > 1:
            logging.info("Updating multiple nodes")
        match_a, properties_filter = querymaker.match_node_query(
            "a", properties_filter=properties_filter
        )
        with_ = querymaker.with_query(["a"])
        set_query, updated_updates = querymaker.patch_property_query(
            "a", updates, change_date
        )
        params = {**properties_filter, **updated_updates}
        query = "\n".join([match_a, with_, set_query])
        db.cypher_query(query, params)
    else:
        logging.error("There isn't such a node to be updated")


def remove_property_from_node(
    id_: str,
    property_kind: str,
    change_date: Optional[datetime.datetime] = None,
):
    """Removes a single property from a given node.

    Args:
      id_: entity id
      property_kind: kind of the property

    Returns:
    Node in case of success or None in case of error.
    """

    property_kinds_list = []
    property_kinds_list.append(property_kind) 

    return delete_properties_from_node(id_, property_kinds_list, change_date)


def delete_properties_from_node(
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

    """
    current_state = get_current_state(id_)
    if not current_state:
        logging.warning("No property was removed. No found node with the specified id")
        return

    updates = {property_:"" for property_ in property_kinds}
    update_node(id_, updates, change_date)

    new_current_state = get_current_state(id_)

    match_state, id_updated = querymaker.match_node_query("state", "State")
    where_state = querymaker.where_node_internal_id_equal_to("state", new_current_state.id)
    remove_state = querymaker.remove_properties_query("state", property_kinds)

    query = "\n".join([match_state, where_state, remove_state])

    db.cypher_query(query, id_updated)


def delete_node(
    id_: str,
    completely=False,
    deletion_date: Optional[datetime.datetime] = None,
):
    """Deletes a node completely from database or make it a thing of the past.

    Args:
      id_: entity id
      completely: if True, then the node will be deleted completely from DB
                  with all its relationships. If False, the node will be marked
                  as deleted by the _deleted property.
      deletion_date: the date of node deletion

    Returns:

    """
    if deletion_date is None:
        deletion_date = datetime.datetime.now()
    properties_filter = {"Id": id_}
    if completely:
        match_a, filter_a = querymaker.match_node_query("a", properties_filter=properties_filter)
        delete_a = querymaker.delete_query("a")

        query = "\n".join([match_a, delete_a])
        params = filter_a

        db.cypher_query(query, params)
    else:
        update_node(properties_filter["Id"], {"_deleted": True}, deletion_date)


def create_relationship(
    id_a: str,
    relationship_kind: str,
    rel_properties: dict,
    id_b: str,
    create_date: Optional[datetime.datetime] = None,
):
    """Finds nodes A and B and set a relationship between them. Direction is from node A to node B.

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
    by_state_properties: bool = True,
) -> list:
    """Searches existing relationships.

    Args:
      relationship_kind: relationship type
      rel_properties_filter: relationship keyword properties for matching
      id_a: id of entity A
      id_b: id of entity B
      limit: maximum # nodes returned
      programmer: False for returning the relationship found. True for returning
                  (query, params) of that relationship matching.

    Returns:
      neo4j relationships list

    """
    if rel_properties_filter is None:
        rel_properties_filter = {}

    if by_state_properties:
        var_name_1 = "a"
        var_name_2 = "b"
    else:
        var_name_1 = "source"
        var_name_2 = "destination"

    match_a, match_b = [""] * 2
    a_properties_filter = {}
    b_properties_filter = {}
    if id_a:
        a_properties_filter = {"Id": id_a}
    if id_b:
        b_properties_filter = {"Id": id_b}
    match_a, filter_a = querymaker.match_node_query(var_name_1, properties_filter=a_properties_filter)
    match_b, filter_b = querymaker.match_node_query(var_name_2, properties_filter=b_properties_filter)
    rel_query, rel_properties_filter = querymaker.match_relationship_query(
        "a", "r", relationship_kind, rel_properties_filter, "b"
    )

    return_ = f"RETURN source, r, destination\nLIMIT {limit}"

    query = "\n".join([match_a, match_b, rel_query, return_])
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
):
    """Deletes a relationship between two nodes A and B.

    Args:
      relationship_kind: relationship type
      id_a: id of entity A
      id_b: id of entity B
      completely: if True, then the relationship will be deleted completely
                  from DB. If False, a new state node will be created via the versioner
                  to indicate a new state without the deleted relationship.
      deletion_date: the date of relationship deletion

    Returns:

    """
    if deletion_date is None:
        deletion_date = datetime.datetime.now()

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

        query = "\n".join([match_a, match_b, delete_query])
        params = {**filter_a, **filter_b}

        db.cypher_query(query, params)


def get_current_state(id_:str) -> Optional[neo4j.graph.Node]: # type: ignore
    """Retrieves the current State node: by a given Entity node.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching

    Returns:
      The "current" node

    """
    match_query, params = querymaker.match_node_query("s", properties_filter={"Id":id_})
    get_query = querymaker.get_current_state_query("s")

    query = "\n".join([match_query, get_query])
    node, _ = db.cypher_query(query, params)
    if node:
        [[node]] = node
        return node
    else:
        return None


ontology_settings = OntologySettings()

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
