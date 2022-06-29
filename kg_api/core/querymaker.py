from typing import Optional
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
) -> Tuple[str, dict]:
    """Prepares and sanitizes graph.versioner.init CYPHER query for node creation.

    Args:
      kind: node kind
      immutable_properties: A Map representing the Entity immutable properties.
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: node creation date

    Returns:
      query string, disambiguated property labels
    """
    kind = sanitize_alphanumeric(kind)
    immutable_properties = sanitize_dict_keys(immutable_properties)
    state_properties = sanitize_dict_keys(state_properties)

    immutable_prop_placeholders = ", ".join(f"{k}: $new_{k}" for k in immutable_properties)
    state_prop_placeholders = ", ".join(f"{k}: $new_{k}" for k in state_properties)

    updated_immutable_properties = {f"new_{k}":v for k,v in immutable_properties.items()}
    updated_state_properties = {f"new_{k}":v for k,v in state_properties.items()}
    params = {**updated_immutable_properties, **updated_state_properties}

    create_date_str = create_date.strftime("%Y-%m-%dT%H:%M:%S.%f")

    query = f"""CALL graph.versioner.init(
        "{kind}", {{{immutable_prop_placeholders}}}, {{{state_prop_placeholders}}},"",
        localdatetime("{create_date_str}")
    )
    YIELD node
    RETURN node
    """
    return query, params


def match_node_query(
    var_name: str, kind: str = "", properties_filter: Optional[dict] = None
) -> Tuple[str, dict]:
    """Prepares and sanitizes MATCH CYPHER query for nodes.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      kind: node kind
      properties_filter: node keyword properties for matching

    Returns:
      query string, disambiguated parameters dict (parameter keys are
      renamed from {key} to {key}_{var_name})

    """
    if properties_filter is None:
        properties_filter = {}
    var_name = sanitize_alphanumeric(var_name)
    properties_filter = sanitize_dict_keys(properties_filter)

    query = f"MATCH ({var_name}"
    specify_kind = ""
    specify_param_placeholders = ")"

    if kind:
        kind = sanitize_alphanumeric(kind)
        specify_kind = f": {kind}"

    if properties_filter:
        param_placeholders = ", ".join(f"{k}: ${k}_{var_name}" for k in properties_filter)
        properties_filter = {f"{k}_{var_name}": v for k, v in properties_filter.items()}
        specify_param_placeholders = f"{{{param_placeholders}}})"
    
    query = "".join([query, specify_kind, specify_param_placeholders])
    return query, properties_filter


def patch_property_query(
    var_name: str,
    updates: dict,
    change_date: datetime.datetime,
    additional_label: str = "",
):
    """Prepares and sanitizes graph.versioner.patch CYPHER query.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      updates: new properties and updated properties
      change_date: the date of making change
      additional_label: The name of an additional Label to the new State

    Returns:
      query string, disambiguated property label

    """
    var_name = sanitize_alphanumeric(var_name)
    additional_label = sanitize_alphanumeric(additional_label)
    updates = sanitize_dict_keys(updates)

    updated_updates = {f"new_{k}_{var_name}": v for k, v in updates.items()}
    prop_placeholders = ", ".join(f"{k}: $new_{k}_{var_name}" for k in updates)

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
    return query, updated_updates


def remove_properties_query(var_name: str, property_kinds: list) -> str:
    """Prepares and sanitizes CYPHER REMOVE query.

    Should be used together with match_query.

    Args:
       var_name: variable name which CYPHER will use to identify the match
       property_kinds: property keys to be deleted
    Returns:
       query string
    """
    var_name = sanitize_alphanumeric(var_name)
    properties_kinds = [sanitize_alphanumeric(kind) for kind in property_kinds]
    query = f"REMOVE {var_name}.{f', {var_name}.'.join(properties_kinds)}"
    return query


def return_nodes_or_relationships_query(var_names: list):
    """Prepares a RETURN CYPHER query to return nodes/relationships.

    Should be used together with match_query.

    Args:
      var_names: A list of variable names which CYPHER will use to identify the match

    Returns:
      query string

    """
    var_names = [sanitize_alphanumeric(var_name) for var_name in var_names]
    query = f"RETURN {', '.join(var_names)}"

    return query


def limit_query(max_limit: int):
    """Prepares CYPHER LIMIT query.
    """
    assert isinstance(max_limit, int)
    query = f"LIMIT {max_limit}"
    return query


def create_relationship_query(
    var_name_a: str,
    relationship_kind: str,
    rel_properties: dict,
    var_name_b: str,
    create_date: datetime.datetime,
) -> Tuple[str, dict]:
    """Prepares and sanitizes versioner's create CYPHER query for relationship creation.
    
    The versioner's create query is graph.versioner.relationship.create.
    Should be used together with match_query.

    Args:
      var_name_a: variable name which CYPHER will use to identify the first node match
      relationship_kind: kind of relationship
      rel_properties: relationship properties
      var_name_b: variable name which CYPHER will use to identify the second node match
      create_date: relationship creation date

    Returns:
      query string, disambiguated property labels

    """
    var_name_a = sanitize_alphanumeric(var_name_a)
    var_name_b = sanitize_alphanumeric(var_name_b)
    relationship_kind = sanitize_alphanumeric(relationship_kind)
    rel_properties = sanitize_dict_keys(rel_properties)

    param_placeholders = ", ".join(f"{k}: $new_{k}" for k in rel_properties)
    updated_rel_properties = {f"new_{k}":v for k,v in rel_properties.items()}

    create_date_str = create_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
    query = f"""CALL graph.versioner.relationship.create(
        {var_name_a},
        {var_name_b},
        "{relationship_kind}",
        {{{param_placeholders}}},
        localdatetime("{create_date_str}")
    )
    YIELD relationship
    RETURN relationship
    """
    return query, updated_rel_properties


def match_relationship_query(
    var_name_a: str,
    var_name_r: str,
    relationship_kind: str,
    rel_properties_filter: dict,
    var_name_b: str,
) -> Tuple[str, dict]:
    """Prepares and sanitizes MATCH CYPHER query for relationships.

    Args:
      var_name_r: variable name which CYPHER will use to identify the relationship match
      var_name_a: variable name which CYPHER will use to identify the first node match
      var_name_b: variable name which CYPHER will use to identify the second node match
      relationship_kind: relationship type
      rel_properties_filter: relationship keyword properties for matching

    Returns:
      query string, disambiguated parameters dict (parameter keys are
      renamed from {key} to {key}_{var_name})

    """
    var_name_r = sanitize_alphanumeric(var_name_r)
    relationship_kind = sanitize_alphanumeric(relationship_kind)
    rel_properties_filter = sanitize_dict_keys(rel_properties_filter)

    param_placeholders = ", ".join(f"{k}: ${k}_{var_name_r}" for k in rel_properties_filter)
    updated_rel_properties_filter = {f"{k}_{var_name_r}": v for k, v in rel_properties_filter.items()}
    query = (
        f"MATCH (source)-[:HAS_STATE]->"
        f"({var_name_a})-[{var_name_r}:{relationship_kind} {{{param_placeholders}}}]->"
        f"({var_name_b})-[:FOR]->(destination)"
    )
    return query, updated_rel_properties_filter


def delete_relationship_query(
    var_name_a: str, relationship_kind: str, var_name_b: str, change_date: datetime.datetime
) -> str:
    """Prepares and sanitizes versioner's delete CYPHER query for relationship deletion.
    
    The versioner's delete query is graph.versioner.relationship.delete.
    Should be used together with match_query.

    Args:
      var_name_a: variable name which CYPHER will use to identify the first node match
      relationship_kind: kind of relationship
      var_name_b: variable name which CYPHER will use to identify the second node match
      change_date: the date of relationship deletion

    Returns:
      query string

    """
    var_name_a = sanitize_alphanumeric(var_name_a)
    var_name_b = sanitize_alphanumeric(var_name_b)
    relationship_kind = sanitize_alphanumeric(relationship_kind)

    change_date_str = change_date.strftime("%Y-%m-%dT%H:%M:%S.%f")

    query = f"""CALL graph.versioner.relationship.delete(
        {var_name_a}, {var_name_b}, "{relationship_kind}", localdatetime("{change_date_str}")
    )
    YIELD result
    RETURN result
    """
    return query


def delete_query(var_name, is_node=True):
    """Prepares DELETE CYPHER query for nodes and relationships.
    
    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      is_node: True for deleting nodes, False for relationships

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    query = f"DELETE {var_name}"
    if is_node:
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


def where_node_internal_id_equal_to(var_name: str, value: int) -> str:
    """Prepares and sanitize WHERE CYPHER query to add a constraint on internal id to the
       patterns in a MATCH clause.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      value: the internal id value to match on

    Returns:
      query the CYPHER instruction form: WHERE id(var_name) = value

    """
    var_name = sanitize_alphanumeric(var_name)
    assert isinstance(value, int), "internal id value should be int"

    query = f"WHERE id({var_name}) = {value}"
    return query


def get_current_state_query(var_name: str) -> str:
    """Prepares and sanitizes versioner's get_current_state node query.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    query = f"CALL graph.versioner.get.current.state({var_name}) YIELD node RETURN node"

    return query

