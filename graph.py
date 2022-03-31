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


def create_relationship(
    kind_a: str, filter_a: dict, relationship: str, kind_b: str, filter_b: dict
):
    """Find nodes A and B and set a relationship between them

    :param kind_a: node A kind
    :param filter_a: node A match filter
    :param relationship: relationship between nodes A and B
    :param kind_b: node B kind
    :param filter_b: node B match filter
    :return:
    """
    match_a, filter_a = querymaker.match_query("a", kind_a, filter_a)
    match_b, filter_b = querymaker.match_query("b", kind_b, filter_b)
    rel = querymaker.merge_relationship_query("a", relationship, "b")

    query = "\n".join([match_a, match_b, rel])
    params = {**filter_a, **filter_b}

    db.cypher_query(query, params)


ontology_settings = OntologySettings()

config.DATABASE_URL = ontology_settings.neo4j_bolt_url
