from neomodel import db, config, clear_neo4j_database
from settings import OntologySettings
import querymaker


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

def search_nodes(kind: str, node_dict: dict = {}, LIMIT=10) -> list:
    """Search existing nodes

    :param kind: node kind
    :param node_dict: node params
    :param LIMIT: maximum # nodes returned 
    :return: neo4j nodes list
    """
    match_a, filter_a = querymaker.match_node_query('a', kind, node_dict)
    return_ = f'RETURN a\n LIMIT {LIMIT}'
    query = '\n'.join([match_a, return_])
    
    nodes, _ = db.cypher_query(query, filter_a)
    return nodes

def create_relationship(
    kind_a: str, filter_a: dict, relationship: str, rel_dict: dict, kind_b: str, filter_b: dict
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
     relationship: str, filter_dict: dict={}, kind_a: str='', filter_a: dict={}, kind_b: str='', 
     filter_b: dict={}, LIMIT=10
) -> list:
    """Search existing relationships

    :param relationship: relationship type
    :param filter_dict: relationship params
    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :param LIMIT: maximum # nodes returned 
    :return: neo4j relationships list
    """
    node_a_query, node_b_query = ['']*2
    if kind_a:
        node_a_query, filter_a = querymaker.match_node_query('a', kind_a, filter_a)
    if kind_b:
        node_b_query, filter_b = querymaker.match_node_query('b', kind_b, filter_b)
    rel_query, filter_dict = querymaker.match_relationship_query('a', 'r', relationship, filter_dict, 'b')
    return_ = f'RETURN a, r, b\nLIMIT {LIMIT}'
    
    query = '\n'.join([node_a_query, node_b_query, rel_query, return_])
    params = {**filter_a, **filter_b, **filter_dict}
    
    rels, _ = db.cypher_query(query, params)
    return rels

ontology_settings = OntologySettings()

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
