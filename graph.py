import logging
from datetime import datetime
from neomodel import db, config, clear_neo4j_database
from settings import OntologySettings
import querymaker
import funcs

NUM_ELEMENTS_IN_REL_OBJ = 3


def drop_database():
    clear_neo4j_database(db)


def create_kind_node(kind: str, node_dict: dict):
    """Create new node

    :param kind: node kind
    :param node_dict: node params
    :return:
    """
    query = querymaker.merge_node_query(kind, node_dict)
    db.cypher_query(query, node_dict)


def search_nodes(kind: str, node_dict: dict = None, limit=10) -> list:
    """Search existing nodes

    :param kind: node kind
    :param node_dict: node params
    :param limit: maximum # nodes returned
    :return: neo4j nodes list
    """
    node_dict = node_dict or {}
    match_a, filter_a = querymaker.match_node_query("a", kind, node_dict)
    return_ = f"RETURN a\n LIMIT {limit}"
    query = "\n".join([match_a, return_])

    nodes, _ = db.cypher_query(query, filter_a)
    return nodes


def update_node(kind: str, updates: dict, change_date, filter_node: dict = None, save=True, debug=True):
    """Update a node properties

    :param kind: node kind
    :param filter_node: node match filter
    :param updates: new properties and updated properties
    :param save: if True the old properties will be saved as history
    :param debug: if True, error messages are activated
    :return:
    """
    filter_node = filter_node or {}
    nodes = search_nodes(kind, filter_node)
    if nodes:
        match_node, filter_node = querymaker.match_node_query("a", kind, filter_node)
        if funcs.check_deletion("a", match_node, filter_node):
            if debug:
                logging.error("You can't update deleted nodes %s %s", kind, filter_node)
            return
        if save:
            properties = funcs.call_properties("a", match_node, filter_node)
            for node in properties.values():
                for prop in node.copy():
                    if prop[0] == "_":
                        node.pop(prop)
            funcs.write_history("a", match_node, filter_node, properties, change_date)
        where, condition_dict = querymaker.where_query("a", {'_deleted':False})
        set_query, updated_updates = querymaker.set_property_query("a", updates)
        params = {**filter_node, **updated_updates, **condition_dict}
        query = "\n".join([match_node, where, set_query])
        db.cypher_query(query, params)


def delete_node(kind: str, node_dict: dict, completely: bool =False):
    """Delete a node completely from database or make it in the past
    :param kind: node kind
    :param node_dict: node params
    :param completely: if True, then the node will be deleted completely
                        from DB with all its relationships.
                        if False, then the node should be just marked as
                        deleted so that no operation could be done on it anymore
    """
    updated_dict={}
    match_a, filter_a = querymaker.match_node_query("a", kind, node_dict)
    if completely:
        delete_a = querymaker.delete_query("a")
    else:
        if funcs.check_deletion("a", match_a, filter_a):
            logging.error("You can't delete deleted nodes")
            return
        delete_a, updated_dict = querymaker.set_property_query(
            'a', {'_deleted':True, 'deletion_timestamp':datetime.now().timestamp()}
        )
    query = "\n".join([match_a, delete_a])
    params = {**filter_a, **updated_dict}

    db.cypher_query(query, params)


def history_lookup_node(kind, filter_node, date: datetime) -> dict:
    """Look at how the node looked like in a specific date

    :param kind: node kind
    :param filter_node: node match filter
    :param date: date in history to look up for
    :return: node properties at the specific date
    """
    var_name="a"
    match_a, filter_node = querymaker.match_node_query(var_name, kind, filter_node)
    return funcs.history_lookup(var_name, match_a, filter_node, date)


def create_relationship(
    kind_a: str,
    filter_a: dict,
    relationship: str,
    rel_dict: dict,
    kind_b: str,
    filter_b: dict,
):
    """Find nodes A and B and set a relationship between them

    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param relationship: relationship between nodes A and B
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :return:
    """
    match_a, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
    match_b, filter_b = querymaker.match_node_query("b", kind_b, filter_b)
    rel = querymaker.merge_relationship_query("a", relationship, rel_dict, "b")

    query = "\n".join([match_a, match_b, rel])
    params = {**filter_a, **filter_b, **rel_dict}

    db.cypher_query(query, params)


def search_relationships(
    relationship: str,
    filter_dict: dict = None,
    kind_a: str = "",
    filter_a: dict = None,
    kind_b: str = "",
    filter_b: dict = None,
    limit=10,
    programmer=0,
) -> list:
    """Search existing relationships

    :param relationship: relationship type
    :param filter_dict: relationship params
    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :param limit: maximum # nodes returned
    :param programmer: False for returning the relationship found.
                        True for returning query, params of matching that relationship.
    :return: neo4j relationships list
    """
    filter_dict = filter_dict or {}
    filter_a = filter_a or {}
    filter_b = filter_b or {}
    node_a_query, node_b_query = [""] * 2
    if kind_a:
        node_a_query, filter_a = querymaker.match_node_query("a", kind_a, filter_a)
    if kind_b:
        node_b_query, filter_b = querymaker.match_node_query("b", kind_b, filter_b)
    rel_query, filter_dict = querymaker.match_relationship_query(
        "a", "r", relationship, filter_dict, "b"
    )
    return_ = f"RETURN a, r, b\nLIMIT {limit}"

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
    change_date,
    filter_rel: dict = None,
    kind_a: str = "",
    filter_a: dict = None,
    kind_b: str = "",
    filter_b: dict = None,
    save = True,
    debug = True,
):
    """Update a relationship properties

    :param relationship: relationship type
    :param updates: new properties and updated properties
    :param filter_rel: relationship params
    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :param save: if True, the old properties will be saved as history
    :param debug: if True, error messages are activated
    :return:
    """
    filter_rel = filter_rel or {}
    filter_a = filter_a or {}
    filter_b = filter_b or {}
    relationships = search_relationships(
        relationship, filter_rel, kind_a, filter_a, kind_b, filter_b
    )
    if relationships:
        match_relationship, params = search_relationships(
            relationship, filter_rel, kind_a, filter_a, kind_b, filter_b, programmer=1
        )
        if funcs.check_deletion("r", match_relationship, params):
            if debug:
                logging.error("You can't update deleted relationships %s %s", relationship, filter_rel)
            return
        if save:
            properties = funcs.call_properties("r", match_relationship, params)
            for rel in properties.values():
                for prop in rel.copy():
                    if prop[0] == "_":
                        rel.pop(prop)
            funcs.write_history("r", match_relationship, params, properties, change_date)
        where, condition_dict = querymaker.where_query("r", {'_deleted':False})
        set_query, updated_updates = querymaker.set_property_query("r", updates)
        query = "\n".join([match_relationship, where, set_query])
        params = {**params, **updated_updates, **condition_dict}
        db.cypher_query(query, params)


def delete_relationship(
    relationship: str,
    filter_rel: dict = None,
    kind_a: str = "",
    filter_a: dict = None,
    kind_b: str = "",
    filter_b: dict = None,
    completely: bool = False,
):
    """Delete a relationship between two nodes A and B
    :param relationship: relationship type
    :param filter_rel: relationship params
    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :param completely: if True, then the relationship will be deleted completely
                        from DB.
                        if False, then it should be just marked as
                        deleted so that no operation could be done on it anymore
    :return:
    """
    filter_rel = filter_rel or {}
    filter_a = filter_a or {}
    filter_b = filter_b or {}
    updated_dict = {}
    match_relationship, params = search_relationships(
        relationship, filter_rel, kind_a, filter_a, kind_b, filter_b, programmer=1
    )
    if completely:
        delete_query = querymaker.delete_query("r", node=False)
    else:
        if funcs.check_deletion("r", match_relationship, params):
            logging.error("You can't delete deleted nodes")
            return
        delete_query, updated_dict = querymaker.set_property_query(
            'r', {'_deleted':True, 'deletion_timestamp':datetime.now().timestamp()}
        )
    params = {**params, **updated_dict}
    query = "\n".join([match_relationship, delete_query])

    db.cypher_query(query, params)


def history_lookup_relationship(
    relationship: str,
    date: datetime,
    filter_rel: dict,
    kind_a: str,
    filter_a: dict,
    kind_b: str,
    filter_b: dict,
):
    """Look at how the relationship looked like in a specific date

    :param relationship: relationship type
    :param date: date in history to look up for
    :param filter_rel: relationship match filter
    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :return: relationship properties at the specific date
    """
    var_name="r"
    match_relationship, params = search_relationships(
        relationship, filter_rel, kind_a, filter_a, kind_b, filter_b, programmer=1
    )
    return funcs.history_lookup(var_name, match_relationship, params, date)


def display_relationships(relationships: list):
    """Display list of relationships in a pretty way
    """
    for relationship in relationships:
        start_node = {k:v for k,v in relationship[0].items()}
        properties ={k:v for k,v in relationship[1].items()}
        end_node = {k:v for k,v in relationship[2].items()}
        if '_history' in properties:
            properties.pop('_history')
        print('{:12s} -[ {:10s} {:20s} ]->\t{:15s}'\
            .format(
                start_node["name"], relationship[1].type, str(properties), end_node["name"])
            )


def get_properties(objects: list) -> dict:
    """Get properties from a list of nodes or relationships objects

    :params objects: a list of nodes or relationships objects
    :return: dictionary of properties
    """
    if not objects:
        return {}
    index = 1 if len(objects[0])==NUM_ELEMENTS_IN_REL_OBJ else 0
    obj_properties = []
    for obj in objects:
        obj ={k:v for k,v in obj[index].items()}
        if '_history' in obj:
            obj.pop('_history')
        obj_properties.append(obj)
    return obj_properties


def display_ontology():
    """Show datamodel (ontology graph)

    :return:
    """
    query = 'call db.schema.visualization()'
    results, _ = db.cypher_query(query)
    [nodes, relationships] = results[0]
    print("\nOntology's Kinds:")
    for node in nodes:
        node = {k:v for k,v in node.items()}
        print(node['name'])

    print("\nOntology's relationships:")
    for rel in relationships:
        start = {k:v for k,v in rel.start_node.items()}
        end = {k:v for k,v in rel.end_node.items()}
        print(start['name'], f'-[{rel.type}]->', end['name'])

ontology_settings = OntologySettings()

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
