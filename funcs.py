from datetime import datetime
import neo4j
from neomodel import db
import querymaker


def check_deletion(var_name: str, match: str, filter_dict: dict) -> bool:
    """Check whether some nodes/relationships are deleted

    :param var_name: nodes' kind
    :params match: CYPHER node/relationship match query
    :param filter_dict: node/relationship match filter
    :return: True if all matched nodes/relationships are deleted
    """
    return_ = f"RETURN {var_name}._deleted"
    query = "\n".join([match, return_])
    nodes, _ = db.cypher_query(query, filter_dict)

    deleted = []
    for node in nodes:
        deleted.append(*node)
    return all(deleted)


def neo4j2timestamp(date: neo4j.time.DateTime):
    """Convert neo4j date into timestamp

    :params date: neo4j date
    :return: date coverted to timestamp
    """
    date = datetime(
        date.year, date.month, date.day, date.hour, date.minute, int(date.second)
    )
    return datetime.timestamp(date)


def call_properties(var_name: str, match: str, filter_dict: dict) -> dict:
    """Call nodes/relationships properties from database

    :params var_name: variable name which CYPHER will use to identify the match
    :params match: CYPHER node/relationship match query
    :param filter_dict: node/relationship match filter
    :return: dictionary of nodes/relationships properties of the form: {node_id/rel_id: props,}
    """
    return_ = f"RETURN id({var_name}), properties({var_name})"
    query = "\n".join([match, return_])
    props_list, _ = db.cypher_query(query, filter_dict)

    properties = {}
    for id_, props in props_list:
        properties[id_]= props

    for id_, props in properties.items():
        for key, value in props.items():
            if isinstance(value, neo4j.time.DateTime):
                props[key] = neo4j2timestamp(value)
    return properties


def write_history(var_name: str, match: str, filter_dict: dict, properties: dict):
    """Add records to the _history property of many nodes/relationships

    :params var_name: variable name which CYPHER will use to identify the match
    :params match: CYPHER node/relationship match query
    :param filter_dict: node/relationship match filter
    :params: properties: nodes/relationships properties to be written of the form: {
        node_id/rel_id: props,
    }
    :return:
    """
    get_history = f"RETURN id({var_name}), apoc.convert.fromJsonMap({var_name}._history)"
    get_history_query = "\n".join([match, get_history])
    histories_list, _ = db.cypher_query(get_history_query, filter_dict)

    histories = {}
    for id_, history in histories_list:
        histories[id_]= history

    for id_, history in histories.copy().items():
        now = int(datetime.now().timestamp())
        if not histories[id_]:
            histories[id_] = {}
        histories[id_][now] = properties[id_]

    set_query, updated_history = querymaker.set_property_query(
        var_name, {"_history": histories}, to_json=True
    )
    set_history_query = "\n".join([match, set_query])

    params = {**filter_dict, **updated_history}
    db.cypher_query(set_history_query, params)


def history_lookup(var_name, match, filter_dict, date):
    """Look at how some nodes/relationships looked like in a specific date

    :params var_name: variable name which CYPHER will use to identify the match
    :params match: CYPHER node/relationship match query
    :param filter_dict: node/relationship match filter
    :param date: date in history to look up for
    :return: node/relationship properties at the specific date of the form: {
        node_id/rel_id: {date: history},
    }
    """
    get_history = f"RETURN id({var_name}), apoc.convert.fromJsonMap({var_name}._history)"
    get_history_query = "\n".join([match, get_history])
    histories_list, _ = db.cypher_query(get_history_query, filter_dict)

    histories = {}
    for id_, history in histories_list:
        histories[id_]= history or {}

    date_story = {}
    for id_, history in histories.items():
        history = {k: history[k] for k in sorted(history)}
        for timestamp in history:
            if date < datetime.fromtimestamp(int(timestamp)):
                date_story[id_] = history[timestamp]
        if id_ not in date_story:
            date_story[id_] = "No history"

    return date_story
