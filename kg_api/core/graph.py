from typing import Optional
import logging
import datetime
import pandas as pd

import neo4j
from neomodel import db, config, clear_neo4j_database
from kg_api.utils.settings import OntologySettings
import kg_api.core.querymaker as querymaker


def drop_database():
    """Clears database."""
    clear_neo4j_database(db)


def create_kind_node(
    kind: str,
    immutable_properties: dict,
    state_properties: dict,
    create_date: Optional[datetime.datetime] = None,
):
    """Creates new node.

    Args:
      kind: node kind
      immutable_properties: A Map representing the Entity immutable properties.
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: node creation date

    Returns:

    """
    if create_date is None:
        create_date = datetime.datetime.now()
    query, params = querymaker.init_node_query(
        kind, immutable_properties, state_properties, create_date
    )

    db.cypher_query(query, params)


def search_nodes(kind: str, properties_filter: Optional[dict] = None, limit=10) -> list:
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


def add_node(
    kind: str,
    id: str,
    properties,
    date_of_creation
):


    return node


def update_node(
    kind: str,
    id: str,
    updated_properties,
    date_of_update
):


    return node


def remove_node(
    kind: str,
    id: str,
    date_of_deletion
)


def add_property_to_node(
    kind: str,
    id: str,
    property_kind: str,
    property_value):
    """Adds a new property to a given node
    """

    # some code here

    return node


def update_property_in_node(
    kind: str,
    id: str,
    property_kind: str,
    property_value):
    """Updates a property for a given node
    """

    # some code here

    return node


def remove_property_from_node(
    kind: str,
    id: str,
    property_kind: str):
    """Removes a property from a given node
    """

    # some code here

    return node


def update_node(
    kind: str,
    updates: dict,
    properties_filter: Optional[dict] = None,
    change_date: Optional[datetime.datetime] = None,
):
    """Updates a node properties.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching
      updates: new properties and updated properties
      change_date: the date of node updating

    Returns:

    """
    if properties_filter is None:
        properties_filter = {}
    if change_date is None:
        change_date = datetime.datetime.now()

    nodes = search_nodes(kind, properties_filter)
    if nodes:
        if len(nodes) > 1:
            logging.info("Updating multiple nodes")
        match_a, properties_filter = querymaker.match_node_query("a", kind, properties_filter)
        with_ = querymaker.with_query(["a"])
        set_query, updated_updates = querymaker.patch_property_query(
            "a", updates, change_date
        )
        params = {**properties_filter, **updated_updates}
        query = "\n".join([match_a, with_, set_query])
        db.cypher_query(query, params)
    else:
        logging.error("There isn't such a node to be updated")


def delete_node(
    kind: str,
    properties_filter: dict,
    completely=False,
    deletion_date: Optional[datetime.datetime] = None,
):
    """Deletes a node completely from database or make it a thing of the past.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching
      completely: if True, then the node will be deleted completely from DB
                  with all its relationships. If False, the node will be marked
                  as deleted by the _deleted property.
      deletion_date: the date of node deletion

    Returns:

    """
    if deletion_date is None:
        deletion_date = datetime.datetime.now()
    if completely:
        match_a, filter_a = querymaker.match_node_query("a", kind, properties_filter)
        delete_a = querymaker.delete_query("a")

        query = "\n".join([match_a, delete_a])
        params = filter_a

        db.cypher_query(query, params)
    else:
        update_node(kind, {"_deleted": True}, properties_filter, deletion_date)


def create_relationship(
    kind_a: str,
    filter_a: dict,
    relationship_kind: str,
    rel_properties: dict,
    kind_b: str,
    filter_b: dict,
    create_date: Optional[datetime.datetime] = None,
):
    """Finds nodes A and B and set a relationship between them.

    Args:
      kind_a: node A kind
      filter_a: node A match filter
      relationship: relationship between nodes A and B
      rel_properties: relationship properties
      kind_b: node B kind
      filter_b: node B match filter
      create_date: relationship creation date

    Returns:

    """
    if create_date is None:
        create_date = datetime.datetime.now()
    match_a, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
    match_b, filter_b = querymaker.match_node_query("b", kind_b, filter_b)
    rel, rel_properties = querymaker.create_relationship_query(
        "a", relationship, rel_properties, "b", create_date
    )
    with_ = querymaker.with_query(["a", "b"])
    query = "\n".join([match_a, match_b, with_, rel])
    params = {**filter_a, **filter_b, **rel_properties}

    db.cypher_query(query, params)


def search_relationships(
    relationship_kind: str,
    rel_properties_filter: Optional[dict] = None,
    kind_a: str = "",
    filter_a: Optional[dict] = None,
    kind_b: str = "",
    filter_b: Optional[dict] = None,
    limit=10,
    programmer=0,
    by_state_properties: bool = True,
) -> list:
    """Searches existing relationships.

    Args:
      relationship: relationship type
      rel_properties_filter: relationship keyword properties for matching
      kind_a: node A kind
      filter_a: node A match filter
      kind_b: node B kind
      filter_b: node B match filter
      limit: maximum # nodes returned
      programmer: False for returning the relationship found. True for returning
                  (query, params) of that relationship matching.

    Returns:
      neo4j relationships list

    """
    if rel_properties_filter is None:
        rel_properties_filter = {}
    if filter_a is None:
        filter_a = {}
    if filter_b is None:
        filter_b = {}
    if by_state_properties:
        var_name_1 = "a"
        var_name_2 = "b"
    else:
        var_name_1 = "source"
        var_name_2 = "destination"

    match_a, match_b = [""] * 2
    match_a, filter_a = querymaker.match_node_query(var_name_1, kind_a, filter_a)
    match_b, filter_b = querymaker.match_node_query(var_name_2, kind_b, filter_b)
    rel_query, rel_properties_filter = querymaker.match_relationship_query(
        "a", "r", relationship, rel_properties_filter, "b"
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
    kind_a: str,
    kind_b: str,
    filter_a: Optional[dict] = None,
    filter_b: Optional[dict] = None,
    change_date: Optional[datetime.datetime] = None,
):
    """Updates a relationship properties.

    Args:
      relationship: relationship type
      updates: new properties and updated properties
      kind_a: node A kind
      filter_a: node A match filter
      kind_b: node B kind
      filter_b: node B match filter
      change_date: the date of node updating

    Returns:

    """
    # - It seems that it's not recommended to update relationship properties, as there is no
    #   special function for doing that in the versioner.
    # - Notice that you need to save the relationship's properties before deletion, in order to
    #   use them in creating the new one.
    if filter_a is None:
        filter_a = {}
    if filter_b is None:
        filter_b = {}
    if change_date is None:
        change_date = datetime.datetime.now()

    delete_relationship(
        relationship, kind_a, kind_b, filter_a, filter_b, deletion_date=change_date
    )
    create_relationship(
        kind_a=kind_a,
        filter_a=filter_a,
        relationship=relationship,
        rel_properties=updates,
        kind_b=kind_b,
        filter_b=filter_b,
        create_date=change_date,
    )


def delete_relationship(
    relationship_kind: str,
    kind_a: str,
    kind_b: str,
    filter_a: Optional[dict] = None,
    filter_b: Optional[dict] = None,
    completely: bool = False,
    deletion_date: Optional[datetime.datetime] = None,
):
    """Deletes a relationship between two nodes A and B.

    Args:
      relationship: relationship type
      kind_a: node A kind
      filter_a: node A match filter
      kind_b: node B kind
      filter_b: node B match filter
      completely: if True, then the relationship will be deleted completely
                  from DB. If False, a new state node will be created via the versioner
                  to indicate a new state without the deleted relationship.
      deletion_date: the date of relationship deletion

    Returns:

    """
    if filter_a is None:
        filter_a = {}
    if filter_b is None:
        filter_b = {}
    if deletion_date is None:
        deletion_date = datetime.datetime.now()

    if completely:
        match_relationship, params = search_relationships(
            relationship,
            kind_a=kind_a,
            filter_a=filter_a,
            kind_b=kind_b,
            filter_b=filter_b,
            programmer=1,
        )
        delete_query = querymaker.delete_query("r", is_node=False)
        query = "\n".join([match_relationship, delete_query])

        db.cypher_query(query, params)
    else:
        match_a, match_b = [""] * 2
        match_a, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
        match_b, filter_b = querymaker.match_node_query("b", kind_b, filter_b)

        delete_query = querymaker.delete_relationship_query(
            "a", relationship, "b", deletion_date
        )

        query = "\n".join([match_a, match_b, delete_query])
        params = {**filter_a, **filter_b}

        db.cypher_query(query, params)


def get_current_state(kind: str, properties_filter: dict) -> list:
    """Retrieves the current State node: by a given Entity node.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching

    Returns:
      List of nodes

    """
    match_query, params = querymaker.match_node_query("s", kind, properties_filter)
    get_query = querymaker.get_current_state_query("s")

    query = "\n".join([match_query, get_query])
    nodes, _ = db.cypher_query(query, params)
    return nodes


def get_all_path(kind: str, properties_filter: dict) -> Optional[neo4j.graph.Path]: # type: ignore
    """Retrieves all Entity's history in a path, including the Entity node,
       all the State nodes, CURRENT and PREVIOUS relationships.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching

    Returns:
      path object

    """
    match, properties_filter = querymaker.match_node_query("a", kind, properties_filter)
    with_ = querymaker.with_query(["a"])
    get = querymaker.get_all_path_query("a")

    query = "\n".join([match, with_, get])
    path, _ = db.cypher_query(query, properties_filter)
    if path:
        return path[0][0]
    else:
        logging.warning("There is no known path for the requested node")
        return None

def get_node_history(kind: str, properties_filter: dict) -> Optional[pd.DataFrame]:
    """Retrieves all changes that happened to a node from its creation moment, using
       the versioner's diff function.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching

    Returns:
      Pandas dataframe consisting of columns: ["operation", "label", "oldValue", "newValue"]

    """
    path = get_all_path(kind, properties_filter)
    if not path:
        logging.error("There is no known history for the requested node")
        return None
    nodes_by_ascending_date = tuple(reversed(path.nodes))

    results = []
    old_new_couples = zip(nodes_by_ascending_date[:-1], nodes_by_ascending_date[1:])
    for old, new in old_new_couples:
        match_from, _ = querymaker.match_node_query("state_from", "State", {})
        match_to, _ = querymaker.match_node_query("state_to", "State", {})

        where_from = querymaker.where_node_internal_id_equal_to("state_from", old.id)
        with_ = querymaker.with_query(["state_from", "state_to"])
        where_to = querymaker.where_node_internal_id_equal_to("state_to", new.id)

        diff = querymaker.diff_query("state_from", "state_to")

        query = "\n".join([match_from, match_to, where_from, with_, where_to, diff])

        list_of_changes, _ = db.cypher_query(query)
        results += list_of_changes + [["","","",""]]

    diff_table = pd.DataFrame(results, columns=["operation", "label", "oldValue", "newValue"])
    return diff_table


def semantic_action_history(kind: str, properties_filter: dict) -> Optional[list]:
    """Retrieves semantic action changes that happened to a node from its creation moment.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching

    Returns:
      List of semantic action changes.

    """
    path = get_all_path(kind, properties_filter)
    if not path:
        logging.error("There is no known semantic action history for the requested node")
        return None
    nodes_by_ascending_date = tuple(reversed(path.nodes))

    semantic_actions = []
    for node in nodes_by_ascending_date:
        semantic_actions.append(node.get("SemanticActionDescription"))
    return semantic_actions

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
