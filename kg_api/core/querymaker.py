from typing import Tuple
import datetime


def sanitize_alphanumeric(input_value: str):
    """Removes characters which are not letters, numbers or underscore.

    Args:
      input_value: raw string

    Returns:

    """
    return "".join(char for char in input_value if char.isalnum() or char == "_")


def sanitize_dict_keys(input_value: dict):
    """Removes characters which are not letters, numbers or underscore from dictionary keys.

    Args:
      input_value: raw dictionary

    Returns:

    """
    return {sanitize_alphanumeric(k): v for k, v in input_value.items()}


def init_node_query(
    kind: str,
    immutable_properties: dict,
    state_properties: dict,
    create_date: datetime.datetime,
) -> str:
    """Prepares and sanitizes graph.versioner.init CYPHER query for node creation.

    Args:
      kind: node kind
      immutable_properties: A Map representing the Entity immutable properties.
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: node creation date

    Returns:
      query string

    """
    kind = sanitize_alphanumeric(kind)
    immutable_properties = sanitize_dict_keys(immutable_properties)
    state_properties = sanitize_dict_keys(state_properties)

    immutable_prop_placeholders = ", ".join(f"{k}: ${k}" for k in immutable_properties)
    state_prop_placeholders = ", ".join(f"{k}: ${k}" for k in state_properties)

    create_date_str = create_date.strftime("%Y-%m-%dT%H:%M:%S.%f")

    query = f"""CALL graph.versioner.init(
        "{kind}", {{{immutable_prop_placeholders}}}, {{{state_prop_placeholders}}},"",
        localdatetime("{create_date_str}")
    )
    YIELD node
    RETURN node
    """
    return query


def match_node_query(var_name: str, kind: str, filter_dict: dict) -> Tuple[str, dict]:
    """Prepares and sanitizes MATCH CYPHER query for nodes.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      kind: node kind
      filter_dict: node keyword arguments for matching

    Returns:
      query string, disambiguated parameters dict (parameter keys are
      renamed from {key} to {key}_{var_name})

    """
    var_name = sanitize_alphanumeric(var_name)
    filter_dict = sanitize_dict_keys(filter_dict)

    param_placeholders = ", ".join(f"{k}: ${k}_{var_name}" for k in filter_dict)
    updated_filter_dict = {f"{k}_{var_name}": v for k, v in filter_dict.items()}
    if kind:
        kind = sanitize_alphanumeric(kind)
        query = f"MATCH ({var_name}:{kind} {{{param_placeholders}}})"
    else:
        query = f"MATCH ({var_name} {{{param_placeholders}}})"
    return query, updated_filter_dict


def patch_property_query(
    var_name: str,
    properties_dict: dict,
    change_date: datetime.datetime,
    additional_label: str = "",
):
    """Prepares and sanitizes graph.versioner.patch CYPHER query.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      properties_dict: the property label to be updated
      change_date: the date of making change
      additional_label: The name of an additional Label to the new State

    Returns:
      query string, disambiguated property label

    """
    var_name = sanitize_alphanumeric(var_name)
    additional_label = sanitize_alphanumeric(additional_label)
    properties_dict = sanitize_dict_keys(properties_dict)

    updated_filter_dict = {f"new_{k}_{var_name}": v for k, v in properties_dict.items()}
    prop_placeholders = ", ".join(f"{k}: $new_{k}_{var_name}" for k in properties_dict)

    change_date_str = change_date.strftime("%Y-%m-%dT%H:%M:%S.%f")

    query = f"""CALL graph.versioner.patch(
        {var_name},
        {{{prop_placeholders}}},
        "{additional_label}",
        localdatetime("{change_date_str}")
    )
    YIELD node
    RETURN node
    """
    return query, updated_filter_dict


def create_relationship_query(
    var_name_a: str,
    relationship: str,
    rel_dict: dict,
    var_name_b: str,
    create_date: datetime.datetime,
) -> str:
    """Prepares and sanitizes versioner's create CYPHER query for relationship creation.
    
    The versioner's create query is graph.versioner.relationship.create.
    Should be used together with match_query.

    Args:
      var_name_a: variable name which CYPHER will use to identify the first node match
      relationship: kind of relationship
      var_name_b: variable name which CYPHER will use to identify the second node match
      create_date: relationship creation date

    Returns:
      query string

    """
    var_name_a = sanitize_alphanumeric(var_name_a)
    var_name_b = sanitize_alphanumeric(var_name_b)
    relationship = sanitize_alphanumeric(relationship)
    rel_dict = sanitize_dict_keys(rel_dict)
    param_placeholders = ", ".join(f"{k}: ${k}" for k in rel_dict)
    create_date_str = create_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
    query = f"""CALL graph.versioner.relationship.create(
        {var_name_a},
        {var_name_b},
        "{relationship}",
        {{{param_placeholders}}},
        localdatetime("{create_date_str}")
    )
    YIELD relationship
    RETURN relationship
    """
    return query


def match_relationship_query(
    var_name_a: str,
    var_name: str,
    relationship: str,
    filter_dict: dict,
    var_name_b: str,
) -> Tuple[str, dict]:
    """Prepares and sanitizes MATCH CYPHER query for relationships.

    Args:
      var_name: variable name which CYPHER will use to identify the relationship match
      var_name_a: variable name which CYPHER will use to identify the first node match
      var_name_b: variable name which CYPHER will use to identify the second node match
      relationship: relationship type
      filter_dict: relationship keyword arguments for matching

    Returns:
      query string, disambiguated parameters dict (parameter keys are
      renamed from {key} to {key}_{var_name})

    """
    var_name = sanitize_alphanumeric(var_name)
    relationship = sanitize_alphanumeric(relationship)
    filter_dict = sanitize_dict_keys(filter_dict)

    param_placeholders = ", ".join(f"{k}: ${k}_{var_name}" for k in filter_dict)
    updated_filter_dict = {f"{k}_{var_name}": v for k, v in filter_dict.items()}
    query = (
        f"MATCH (source)-[:HAS_STATE]->"
        f"({var_name_a})-[{var_name}:{relationship} {{{param_placeholders}}}]->"
        f"({var_name_b})-[:FOR]->(destination)"
    )
    return query, updated_filter_dict


def delete_relationship_query(
    var_name_a: str, relationship: str, var_name_b: str, change_date: datetime.datetime
) -> str:
    """Prepares and sanitizes versioner's delete CYPHER query for relationship deletion.
    
    The versioner's delete query is graph.versioner.relationship.delete.
    Should be used together with match_query.

    Args:
      var_name_a: variable name which CYPHER will use to identify the first node match
      relationship: kind of relationship
      var_name_b: variable name which CYPHER will use to identify the second node match
      change_date: the date of relationship deletion

    Returns:
      query string

    """
    var_name_a = sanitize_alphanumeric(var_name_a)
    var_name_b = sanitize_alphanumeric(var_name_b)
    relationship = sanitize_alphanumeric(relationship)

    change_date_str = change_date.strftime("%Y-%m-%dT%H:%M:%S.%f")

    query = f"""CALL graph.versioner.relationship.delete(
        {var_name_a}, {var_name_b}, "{relationship}", localdatetime("{change_date_str}")
    )
    YIELD result
    RETURN result
    """
    return query


def delete_query(var_name, node=True):
    """Prepares DELETE CYPHER query for nodes and relationships.
    
    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      node: True for deleting nodes, False for relationships

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    query = f"DELETE {var_name}"
    if node:
        query = "DETACH " + query
    return query


def with_query(var_names: list) -> str:
    """Prepares WITH CYPHER query to chain queries togther
    
    Should be used together with match_query.

    Args:
      var_names: list of variable names to pipe to the next query

    Returns:
      query string

    """
    query = "WITH "
    var_names = [sanitize_alphanumeric(var_name) for var_name in var_names]
    var_names_str = ", ".join(var_names)

    query = query + var_names_str
    return query