from typing import Tuple


def sanitize_alphanumeric(input_value: str):
    """Remove characters which are not letters, numbers or underscore

    :param input_value: raw string
    :return:
    """
    return "".join(char for char in input_value if char.isalnum() or char == "_")


def sanitize_dict_keys(input_value: dict):
    """Remove characters which are not letters, numbers or underscore from dictionary keys

    :param input_value: raw dictionary
    :return:
    """
    return {sanitize_alphanumeric(k): v for k, v in input_value.items()}


def merge_node_query(kind: str, node_dict: dict) -> str:
    """Prepare and sanitize MERGE CYPHER query for node creation.

    :param kind: node kind
    :param node_dict: node keyword arguments
    :return: query string
    """
    kind = sanitize_alphanumeric(kind)
    node_dict = sanitize_dict_keys(node_dict)

    param_placeholders = ", ".join(f"{k}: ${k}" for k in node_dict.keys())
    query = f"MERGE (:{kind} {{{param_placeholders}}})"
    return query


def match_node_query(var_name: str, kind: str, filter_dict: dict) -> Tuple[str, dict]:
    """Prepare and sanitize MATCH CYPHER query for nodes.

    :param var_name: variable name which CYPHER will use to identify the match
    :param kind: node kind
    :param filter_dict: node keyword arguments for matching
    :return: query string, disambiguated parameters dict (parameter keys are renamed from {key} to {key}_{var_name})
    """
    var_name = sanitize_alphanumeric(var_name)
    kind = sanitize_alphanumeric(kind)
    filter_dict = sanitize_dict_keys(filter_dict)

    param_placeholders = ", ".join(f"{k}: ${k}_{var_name}" for k in filter_dict.keys())
    updated_filter_dict = {f"{k}_{var_name}": v for k, v in filter_dict.items()}
    query = f"MATCH ({var_name}:{kind} {{{param_placeholders}}})"
    return query, updated_filter_dict

def set_property_query(var_name: str, property_: str):
    """Prepare and sanitize SET CYPHER query.
    :params var_name: variable name which CYPHER will use to identify the match
    :params property_: the property label to be updated
    :return: query string, disambiguated property label
    """
    var_name = sanitize_alphanumeric(var_name)
    property_ = sanitize_alphanumeric(property_)

    updated_property = f"new_{property_}_{var_name}"
    query = f"SET {var_name}.{property_} = ${updated_property}"
    return query, updated_property


def merge_relationship_query(
    var_name_a: str, relationship: str, rel_dict: dict, var_name_b: str
) -> str:
    """Prepare and sanitize MERGE CYPHER query for relationship creation.
    Should be used together with match_query.

    :param var_name_a: variable name which CYPHER will use to identify the first node match
    :param relationship: kind of relationship
    :param var_name_b: variable name which CYPHER will use to identify the second node match
    :return: query string
    """
    var_name_a = sanitize_alphanumeric(var_name_a)
    var_name_b = sanitize_alphanumeric(var_name_b)
    relationship = sanitize_alphanumeric(relationship)
    rel_dict = sanitize_dict_keys(rel_dict)
    param_placeholders = ', '.join(f'{k}: ${k}' for k in rel_dict.keys())
    query = f"MERGE ({var_name_a})-[:{relationship} {{{param_placeholders}}}]->({var_name_b})"
    return query

def match_relationship_query(
    var_name_a: str, var_name: str, relationship: str, filter_dict: dict, var_name_b: str
) -> Tuple[str, dict]:
    """Prepare and sanitize MATCH CYPHER query for relationships.

    :param var_name: variable name which CYPHER will use to identify the relationship match
    :param var_name_a: variable name which CYPHER will use to identify the first node match
    :param var_name_b: variable name which CYPHER will use to identify the second node match
    :param relationship: relationship type
    :param filter_dict: relationship keyword arguments for matching
    :return: query string, disambiguated parameters dict (parameter keys are renamed from {key} to {key}_{var_name})
    """
    var_name = sanitize_alphanumeric(var_name)
    relationship = sanitize_alphanumeric(relationship)
    filter_dict = sanitize_dict_keys(filter_dict)

    param_placeholders = ", ".join(f"{k}: ${k}_{var_name}" for k in filter_dict.keys())
    updated_filter_dict = {f"{k}_{var_name}": v for k, v in filter_dict.items()}
    query = f"MATCH ({var_name_a})-[{var_name}:{relationship} {{{param_placeholders}}}]->({var_name_b})"
    return query, updated_filter_dict

def delete_query(var_name, node=True):
    """Prepare DELETE CYPHER query for nodes and relationships.
    :params var_name: variable name which CYPHER will use to identify the match
    :params node: True for deleting nodes, False for relationships
    :return: query string
    """
    query = f'DELETE {var_name}'
    if node:
        query = 'DETACH ' + query
    return query