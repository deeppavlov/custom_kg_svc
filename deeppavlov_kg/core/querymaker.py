import datetime
from typing import List, Optional, Tuple


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


def sanitize_id(input_value: str):
    """Removes characters which are not letters, numbers, underscore or dash.

    Args:
      input_value: raw string

    Returns:

    """
    return "".join(char for char in input_value if char.isalnum() or char in ["_","-"])


def verify_date_validity(date_: str):
    """Verifies that a given date is of format "%Y-%m-%dT%H:%M:%S"."""
    try:
        datetime.datetime.strptime(date_, "%Y-%m-%dT%H:%M:%S")
    except Exception as exp:
        raise exp


def init_entity_query(
    kind: str,
    immutable_properties: dict,
    state_properties: dict,
    create_date: datetime.datetime,
) -> Tuple[str, dict]:
    """Prepares and sanitizes graph.versioner.init CYPHER query for entity creation.

    Args:
      kind: entity kind
      immutable_properties: A Map representing the Entity immutable properties.
      state_properties: A Map representing the Entity state properties (mutable).
      create_date: entity creation date

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


def set_property_query(var_name: str, properties_dict: dict):
    """Prepare and sanitize SET CYPHER query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      property_: the property label to be updated

    Returns:
      query string, disambiguated property label

    """
    var_name = sanitize_alphanumeric(var_name)
    properties_dict = sanitize_dict_keys(properties_dict)

    updated_filter_dict = {f"new_{k}_{var_name}": v for k, v in properties_dict.items()}
    param_placeholders = ", ".join(
        f"{var_name}.{k}= $new_{k}_{var_name}" for k in properties_dict.keys()
    )
    query = f"SET {param_placeholders}"

    return query, updated_filter_dict


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
    """Prepares a RETURN CYPHER query to return entities/relationships.

    Should be used together with match_query.

    Args:
      var_names: A list of variable names which CYPHER will use to identify the match

    Returns:
      query string

    """
    var_names = [sanitize_alphanumeric(v) for v in var_names]
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


def match_relationship_cypher_query(
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
    specify_kind = ""
    if relationship_kind:
        relationship_kind = sanitize_alphanumeric(relationship_kind)
        specify_kind = f": {relationship_kind}"
    rel_properties_filter = sanitize_dict_keys(rel_properties_filter)

    param_placeholders = ", ".join(f"{k}: ${k}_{var_name_r}" for k in rel_properties_filter)
    updated_rel_properties_filter = {
        f"{k}_{var_name_r}": v for k, v in rel_properties_filter.items()
    }
    query = (
        f"MATCH ({var_name_a})"
        f"-[{var_name_r} {specify_kind} {{{param_placeholders}}}]->"
        f"({var_name_b})"
    )
    return query, updated_rel_properties_filter


def match_relationship_versioner_query(
    var_name_a: str,
    var_name_r: str,
    relationship_kind: str,
    rel_properties_filter: dict,
    var_name_b: str,
    state_relationship_kind: str,
) -> Tuple[str, dict]:
    """Prepares MATCH query for relationships, taking into consideration versioner
       nodes and relationship like CURRENT, FOR, etc. that permeate the dataset.

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
    state_node_query, _ = match_node_query("state", "State")
    current_rel_query, _ = match_relationship_cypher_query(
        var_name_a=var_name_a,
        var_name_r=state_relationship_kind.lower(),
        relationship_kind=state_relationship_kind,
        rel_properties_filter={},
        var_name_b="state"
    )
    r_node_query, _ = match_node_query("r_node", "R")
    relationship_query, rel_properties_filter = match_relationship_cypher_query(
        var_name_a="state",
        var_name_r=var_name_r,
        relationship_kind=relationship_kind,
        rel_properties_filter=rel_properties_filter,
        var_name_b='r_node'
    )
    for_rel_query, _ = match_relationship_cypher_query(
        var_name_a="r_node",
        var_name_r="for_node",
        relationship_kind="FOR",
        rel_properties_filter={},
        var_name_b=var_name_b,
    )
    query = "\n".join([
        state_node_query, current_rel_query, r_node_query, relationship_query, for_rel_query
    ])
    return query, rel_properties_filter


def delete_relationship_versioner_query(
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
    """
    return query


def delete_relationship_cypher_query(var_name):
    """Prepares DELETE CYPHER query for relationships.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    query = f"DELETE {var_name}"
    return query


def delete_node_query(var_name):
    """Prepares DELETE CYPHER query for nodes, deleting a node and all its related relationships.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    query = f"DETACH DELETE {var_name}"
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
    var_names = [sanitize_alphanumeric(v) for v in var_names]
    var_names_str = ", ".join(var_names)

    query = query + var_names_str
    return query


def where_internal_id_equal_to(var_names: List[str], values: List[int]) -> str:
    """Prepares and sanitize WHERE CYPHER query to add a constraint on internal id to the
       patterns in a MATCH clause.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      value: the internal id value to match on

    Returns:
      query the CYPHER instruction form: WHERE id(var_name) = value

    """
    if len(var_names) != len(values):
        logging.error("Number of var_names and values should be equal")
        return ""
    var_names = [sanitize_alphanumeric(var_name) for var_name in var_names]
    for value in values:
        assert isinstance(value, int), "internal id value should be int"

    query = f"id({var_names.pop()}) = {values.pop()}"
    for var_name, value in zip(var_names, values):
        query = " and ".join([query, f"id({var_name}) = {value}"])
    query = " ".join(["WHERE", query])
    return query


def where_property_value_in_list_query(var_name:str, property_kind:"str", values:list) -> str:
    """Prepares and sanitizes WHERE-IN CYPHER query.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match
      property_kind: Property to filter on
      values: valid values of property

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    property_kind = sanitize_alphanumeric(property_kind)
    values = [sanitize_id(v) for v in values]
    query = f"WHERE {var_name}.{property_kind} IN {values}"
    return query


def where_entity_kind_in_list_query(var_name:str, kinds:list) -> str:
    """Prepares and sanitizes a query to check if any kind in "kinds" is in var_name node kinds.

    Should be

    Args:
      var_name: variable name which CYPHER will use to identify the match
      kinds: list of kinds to check their equality to the matched node's kind

    Returns
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    kinds = [sanitize_id(k) for k in kinds]
    constraints = [f"'{k}' in labels({var_name})" for k in kinds]
    query = " OR ".join(constraints)
    query = " ".join(["WHERE", query])
    return query


def where_state_on_date(date_: str) -> str:
    """Prepares a WHERE CYPHER query to add a constraint on startDate and endDate properties
       of HAS_STATE relationship.

    The constraint is in format: startDate <= date_ <= endDate

    Args:
      date_: date in format: "%Y-%m-%dT%H:%M:%S"

    Returns:
      query string

    """
    verify_date_validity(date_)
    query = (f"WHERE has_state.startDate <=localdatetime('{date_}')"
             f" and (has_state.endDate >=localdatetime('{date_}') or has_state.endDate is null)")
    return query


def get_current_state_query(var_name: str) -> str:
    """Prepares and sanitizes versioner's get_current_state_node query.

    Should be used together with match_query.

    Args:
      var_name: variable name which CYPHER will use to identify the match

    Returns:
      query string

    """
    var_name = sanitize_alphanumeric(var_name)
    query = f"CALL graph.versioner.get.current.state({var_name}) YIELD node"

    return query

def get_property_differences_query(state_from: str, state_to: str) -> str:
    """Prepares and sanitizes versioner's diff query.

    Args:
      state_from: variable name of the first state
      state_to: variable name of the second state

    Returns:
      query string
    """
    state_from = sanitize_alphanumeric(state_from)
    state_to = sanitize_alphanumeric(state_to)
    query = f"""
        CALL graph.versioner.diff({state_from}, {state_to})
        YIELD operation, label, oldValue, newValue
    """
    return query
