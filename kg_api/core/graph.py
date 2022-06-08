from typing import Optional
import datetime
from neomodel import db, config, clear_neo4j_database
from kg_api.utils.settings import OntologySettings
import kg_api.core.querymaker as querymaker


def drop_database():
    """Clear database."""
    clear_neo4j_database(db)


def create_kind_node(
    kind: str,
    immutable_properties: dict,
    state_properties: dict,
    create_date: Optional[datetime.datetime] = None,
):
    """Create new node.

    Args:
      kind: node kind
      immutable_properties: A Map representing the Entity immutable properties.
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: node creation date

    Returns:

    """
    if create_date is None:
        create_date = datetime.datetime.now()
    query = querymaker.init_node_query(
        kind, immutable_properties, state_properties, create_date
    )
    node_dict = {**immutable_properties, **state_properties}
    db.cypher_query(query, node_dict)


def search_nodes(kind: str, node_dict: Optional[dict] = None, limit=10) -> list:
    """Search existing nodes.

    Args:
      kind: node kind
      node_dict: node params
      limit: maximum number of returned nodes

    Returns:
      neo4j nodes list

    """
    if node_dict is None:
        node_dict = {}
    match_a, filter_a = querymaker.match_node_query("a", kind, node_dict)
    return_ = f"RETURN a\n LIMIT {limit}"
    query = "\n".join([match_a, return_])

    nodes, _ = db.cypher_query(query, filter_a)
    return nodes


def update_node(
    kind: str,
    updates: dict,
    filter_node: Optional[dict] = None,
    change_date: Optional[datetime.datetime] = None,
):
    """Update a node properties.

    Args:
      kind: node kind
      filter_node: node match filter
      updates: new properties and updated properties
      change_date: the date of node updating

    Returns:

    """
    if filter_node is None:
        filter_node = {}
    if change_date is None:
        change_date = datetime.datetime.now()

    nodes = search_nodes(kind, filter_node)
    if len(nodes) == 1:  # we need to update exactly one node
        match_node, filter_node = querymaker.match_node_query("a", kind, filter_node)
        with_ = querymaker.with_query(["a"])
        set_query, updated_updates = querymaker.patch_property_query(
            "a", updates, change_date
        )
        params = {**filter_node, **updated_updates}
        query = "\n".join([match_node, with_, set_query])
        db.cypher_query(query, params)


def delete_node(
    kind: str,
    node_dict: dict,
    completely=False,
    deletion_date: Optional[datetime.datetime] = None,
):
    """Delete a node completely from database or make it a thing of the past.

    Args:
      kind: node kind
      node_dict: node params
      completely: if True, then the node will be deleted completely from DB
                  with all its relationships. If False, the node will be marked
                  as deleted by the _deleted property.
      deletion_date: the date of node deletion

    Returns:

    """
    if deletion_date is None:
        deletion_date = datetime.datetime.now()
    if completely:
        match_a, filter_a = querymaker.match_node_query("a", kind, node_dict)
        delete_a = querymaker.delete_query("a")

        query = "\n".join([match_a, delete_a])
        params = filter_a

        db.cypher_query(query, params)
    else:
        update_node(kind, {"_deleted": True}, node_dict, deletion_date)


def create_relationship(
    kind_a: str,
    filter_a: dict,
    relationship: str,
    rel_dict: dict,
    kind_b: str,
    filter_b: dict,
    create_date: Optional[datetime.datetime] = None,
):
    """Find nodes A and B and set a relationship between them.

    Args:
      kind_a: node A kind
      filter_a: node A match filter
      relationship: relationship between nodes A and B
      kind_b: node B kind
      filter_b: node B match filter
      create_date: relationship creation date

    Returns:

    """
    if create_date is None:
        create_date = datetime.datetime.now()
    match_a, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
    match_b, filter_b = querymaker.match_node_query("b", kind_b, filter_b)
    rel = querymaker.create_relationship_query(
        "a", relationship, rel_dict, "b", create_date
    )
    with_ = querymaker.with_query(["a", "b"])
    query = "\n".join([match_a, match_b, with_, rel])
    params = {**filter_a, **filter_b, **rel_dict}

    db.cypher_query(query, params)


def search_relationships(
    relationship: str,
    filter_dict: Optional[dict] = None,
    kind_a: str = "",
    filter_a: Optional[dict] = None,
    kind_b: str = "",
    filter_b: Optional[dict] = None,
    limit=10,
    programmer=0,
) -> list:
    """Search existing relationships.

    Args:
      relationship: relationship type
      filter_dict: relationship params
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
    if filter_dict is None:
        filter_dict = {}
    if filter_a is None:
        filter_a = {}
    if filter_b is None:
        filter_b = {}
    node_a_query, node_b_query = [""] * 2
    if kind_a:
        node_a_query, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
    if kind_b:
        node_b_query, filter_b = querymaker.match_node_query("b", kind_b, filter_b)
    rel_query, filter_dict = querymaker.match_relationship_query(
        "a", "r", relationship, filter_dict, "b"
    )
    return_ = f"RETURN source, r, destination\nLIMIT {limit}"

    query = "\n".join([node_a_query, node_b_query, rel_query, return_])
    params = {**filter_a, **filter_b, **filter_dict}

    if programmer:
        query = "\n".join([node_a_query, node_b_query, rel_query])
        return query, params

    rels, _ = db.cypher_query(query, params)
    return rels


def update_relationship(
    relationship: str,
    updates: dict,
    kind_a: str,
    kind_b: str,
    filter_a: Optional[dict] = None,
    filter_b: Optional[dict] = None,
    change_date: Optional[datetime.datetime] = None,
):
    """Update a relationship properties.

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
        rel_dict=updates,
        kind_b=kind_b,
        filter_b=filter_b,
        create_date=change_date,
    )


def delete_relationship(
    relationship: str,
    kind_a: str,
    kind_b: str,
    filter_a: Optional[dict] = None,
    filter_b: Optional[dict] = None,
    completely: bool = False,
    deletion_date: Optional[datetime.datetime] = None,
):
    """Delete a relationship between two nodes A and B.

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
        delete_query = querymaker.delete_query("r", node=False)
        query = "\n".join([match_relationship, delete_query])

        db.cypher_query(query, params)
    else:
        node_a_query, node_b_query = [""] * 2
        node_a_query, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
        node_b_query, filter_b = querymaker.match_node_query("b", kind_b, filter_b)

        delete_query = querymaker.delete_relationship_query(
            "a", relationship, "b", deletion_date
        )

        query = "\n".join([node_a_query, node_b_query, delete_query])
        params = {**filter_a, **filter_b}

        db.cypher_query(query, params)


ontology_settings = OntologySettings()

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
