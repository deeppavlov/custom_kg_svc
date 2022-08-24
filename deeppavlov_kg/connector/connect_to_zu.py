import copy
import datetime
import json
import logging
import time

import neo4j
import redis

from deeppavlov_kg import KnowledgeGraph
from deeppavlov_kg.connector.read_external_data import read_aof, read_all_redis_keys, read_redis_value
from deeppavlov_kg.connector.semantic_action_kinds import get_semantic_action_desc
from deeppavlov_kg.core import querymaker

WAITING_TIME_TO_REPEAT_PORT_CHECKING = 5
UNLIMITED = 10**10

DELETED_THIS_ITEM = 72
MOVE_IN_SPACE = 74
UNWANTED_SEMANTIC_ACTIONS = [MOVE_IN_SPACE, DELETED_THIS_ITEM]


def deflate_dicts(dict_: dict) -> dict:
    """Converts dictionaries inside a dictionary to keys-values in the outer dictionary.

    The form of conversion is:
    dict_={"inner_dict":{"key":"value"}} -> dict_={"inner_dict_key":"value"}

    Args:
      dict_: The outer dictionary that has properties to convert

    Returns:
      The outer dictionary after convertion
    """
    for key,value in dict_.copy().items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                dict_.update({"_".join([key,inner_key]):inner_value})
            dict_.pop(key)
    return dict_


def deflate_lists(dict_: dict) -> dict:
    """Converts lists inside a dictionary to json-dumped string in the outer dictionary

    The form of conversion is:
    dict_={"inner_list":["value1","value2"]} -> dict_={"inner_list":'["value1","value2"]'}

    Args:
      dict_: The outer dictionary that has properties to convert

    Returns:
      The outer dictionary after convertion
    """
    for key,value in dict_.copy().items():
        if isinstance(value, list):
            value = json.dumps(value)
            dict_.update({key:value})
    return dict_


def escape_nones(dict_: dict) -> dict:
    """Gets rid of none values inside a dictionary.

    By filling them with empty string

    Args:
      dict_: The outer dictionary that has properties to convert

    Returns:
      The outer dictionary after convertion
    """
    for key,value in dict_.copy().items():
        if value is None:
            value = ""
            dict_.update({key:value})
    return dict_


def neo4j2datetime(date_: neo4j.time.DateTime):  # type: ignore
    """Converts neo4j date into Python datetime

    Args:
      date: neo4j date

    Returns:
      date coverted to datetime

    """
    date_ = datetime.datetime(
        date_.year, date_.month, date_.day, date_.hour, date_.minute, int(date_.second)
    )
    return date_


def check_updates_novelty(graph: KnowledgeGraph, kind: str, properties_filter: dict, updates: dict) -> bool:
    """Checks the current state of a node and compare it with the updates.

    Args:
      kind: node kind
      properties_filter: node keyword properties for matching
      updates: new properties and updated properties

    Returns:
      False, if updates are the same with current state. True otherwise

    """
    current_state = graph.get_current_state(properties_filter["Id"])
    if current_state:
        if current_state["SemanticAction"] == updates["SemanticAction"]:
            if (
                neo4j2datetime(current_state["TLChange"]) ==
                updates["TLChange"].replace(microsecond=0)
            ):
                logging.warning("An attempt to update using the same property value of TLChange")
                return False
    return True


def ignore_semantic_actions(unwanted_semantic_actions: list, nodes: list) -> list:
    """Deletes nodes, that have unwanted semantic actions, from a list of nodes.

    Args:
      unwanted_semantic_actions: Semantic action numbers to ignore
      nodes: The list of nodes to delete the semantic actions from

    Returns:
      Nodes list after cleaning
    """
    clean_nodes = []
    for node in nodes:
        if node["SemanticAction"] not in unwanted_semantic_actions:
            clean_nodes.append(node)
    return clean_nodes


def insert_into_kg(graph: KnowledgeGraph, nodes: list):
    """Writes nodes and relationships to neo4j database.

    Args:
      nodes: List of nodes, containing relationships in key "Related"

    Returns:

    """
    nodes2 = copy.deepcopy(nodes)

    all_db_nodes = graph.search_for_entities(limit=UNLIMITED)
    added_ids = set([node.pop().get("Id") for node in all_db_nodes])
    if added_ids:
        added_ids.remove(None) # Get rid of State and R nodes (without Id)

    for node in nodes:
        node["TLChange"] = datetime.datetime.strptime(node["TLChange"][:-2], "%Y-%m-%dT%H:%M:%S.%f")
    nodes = sorted(nodes, key=lambda item: item["TLChange"])

    for node in nodes.copy():
        create_date = datetime.datetime.strptime(
            node.pop("Created")[:-2], "%Y-%m-%dT%H:%M:%S.%f"
        )
        node.pop("Keyphrases")
        node.pop("Related")

        node = deflate_dicts(node)
        node = deflate_lists(node)
        node = escape_nones(node)

        immutable = {}
        immutable_keys = [
            "AppId",
            # "ClusterId",
            # "HoldingAppSourceBoundListId",
            # "HoldingListId",
            "Id",
            # "IsAppSourceBoundList",
            # "IsList",
            # "IsNotInSpace",
        ]
        for property_key in node.copy():
            if property_key in immutable_keys:
                immutable.update({property_key:node.pop(property_key)})

        mutable = {}
        mutable.update(node)
        mutable["SemanticActionDescription"] = get_semantic_action_desc(mutable["SemanticAction"])

        node_kind = mutable.pop("Kind")
        if node_kind in [
            "ZetUniverse.Kinds.Topic",
            "ZetUniverse.Kinds.UserIdentificator",
            "ZetUniverse.Kinds.Views.GraphView"
        ]:
            continue
        if immutable["Id"] not in added_ids:
            property_types = [type(value) for value in mutable.values()]
            graph.ontology.create_entity_kind(
                kind=node_kind,
                kind_properties=list(mutable.keys()),
                kind_property_types=property_types,
            )
            graph.create_entity(
                kind=node_kind,
                id_=immutable["Id"],
                property_kinds=list(mutable.keys()),
                property_values=list(mutable.values()),
                create_date=create_date,
            )
            added_ids.add(immutable["Id"])
        else:
            if check_updates_novelty(
                graph,
                kind=node_kind,
                updates=mutable,
                properties_filter=immutable,
            ):
                graph.create_or_update_properties_of_entity(
                    id_=immutable["Id"],
                    list_of_property_kinds=list(mutable.keys()),
                    list_of_property_values=list(mutable.values()),
                    change_date=mutable["TLChange"],
                )

    for node in nodes2:
        related = node.pop("Related")
        if related:
            for relationship_dict in related:
                relationship_dict.pop("RelationshipEntityId")
                relationship_type = relationship_dict.pop("Relation")
                node2_id = relationship_dict.pop("EntityId")

                if relationship_dict["Direction"]:
                    id_from = node["Id"]
                    id_to = node2_id
                else:
                    id_from = node2_id
                    id_to = node["Id"]
                relationship_dict.pop("Direction")

                if not graph.search_relationships(
                    relationship_kind=relationship_type,
                    id_a = id_from,
                    id_b = id_to,
                ):
                    if (
                        (entity_a:=graph.get_entity_by_id(id_from)) is not None
                        and (entity_b:=graph.get_entity_by_id(id_to)) is not None
                    ):
                        property_types = [type(value) for value in relationship_dict.values()]
                        graph.ontology.create_relationship_kind(
                            relationship_kind=relationship_type,
                            kind_a=next(iter(entity_a.labels)),
                            kind_b=next(iter(entity_b.labels)),
                            rel_property_kinds=list(relationship_dict.keys()),
                            rel_property_types=property_types,
                            
                        )
                        graph.create_relationship(
                            id_a=id_from,
                            relationship_kind=relationship_type,
                            id_b=id_to,
                            rel_property_kinds=list(relationship_dict.keys()),
                            rel_property_values=list(relationship_dict.values()),
                    )
                else:
                    logging.warning(
                        "An attempt to create a relationship between two "
                        "nodes that has the same relationship (%s)-[%s]->(%s)",
                        id_from, relationship_type, id_to
                    )


def generate_from_aof(graph: KnowledgeGraph, file_path: str):
    """Reads aof file and inserts its data to neo4j database.

    Args:
      file_path: The aof file path in system

    Returns:

    """
    graph.drop_database()
    nodes = read_aof(file_path)
    nodes = ignore_semantic_actions(UNWANTED_SEMANTIC_ACTIONS, nodes)
    insert_into_kg(graph, nodes)


def connect_to_redis(graph: KnowledgeGraph, port: int):
    """Connects to redis port, reads data from it, and inserts it in neo4j database along the
       way as long as the port is open.

    Args:
      port: Redis port to connect to

    Returns:

    """
    r_db = redis.Redis(port=port)
    old_keys = read_all_redis_keys(r_db)

    for _ in range(200):
        time.sleep(WAITING_TIME_TO_REPEAT_PORT_CHECKING)
        logging.info("Reading data")
        current_keys = read_all_redis_keys(r_db)

        number_new_keys = len(current_keys) - len(old_keys)
        if number_new_keys:
            new_keys = current_keys[-number_new_keys:]
        else:
            new_keys = []

        redis_data = {}
        for key in new_keys:
            properties_dict = read_redis_value(key, r_db)
            redis_data[key] = properties_dict

        if redis_data:
            nodes = list(redis_data.values())
            nodes = ignore_semantic_actions(UNWANTED_SEMANTIC_ACTIONS, nodes)
            insert_into_kg(graph, nodes)

            titles = {node["Title"] for node in nodes}
            if titles:
                logging.info("Changes about %s have been inserted in neo4j DB", titles)

        old_keys = current_keys
