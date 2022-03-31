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


def match_query(var_name: str, kind: str, filter_dict: dict) -> Tuple[str, dict]:
    """Prepare and sanitize MATCH CYPHER query.

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


def merge_relationship_query(
    var_name_a: str, relationship: str, var_name_b: str
) -> str:
    """Prepare and sanitize MERGE CYPHER query for relationship creation.
    Should be used together with match_query.

    :param var_name_a: variable name which CYPHER will use to identify the first match
    :param relationship: kind of relationship
    :param var_name_b: variable name which CYPHER will use to identify the second match
    :return: query string
    """
    var_name_a = sanitize_alphanumeric(var_name_a)
    var_name_b = sanitize_alphanumeric(var_name_b)
    relationship = sanitize_alphanumeric(relationship)

    query = f"MERGE ({var_name_a})-[:{relationship}]->({var_name_b})"
    return query
