import datetime
from random import randint
from typing import Tuple, Generator

from fabulist import Fabulist
import mimesis.random as rand
from mimesis import Generic, Numeric
from mimesis.locales import Locale
from mimesis.schema import Field, Schema

import kg_api.core.graph as graph

generic = Generic(locale=Locale.EN)
fabulist = Fabulist()

NODE_LABELS = [[fabulist.get_word("noun").capitalize()] for _ in range(10)]
RELATIONSHIP_LABELS = [[fabulist.get_word("verb").upper()] for _ in range(10)]
# TODO: Add USER_RELATIONSHIP_LABELS in order to add more user properities to deepen dataset.
# Properties like: ["INTERESTED_IN", "LIKES", "DISLIKES", "HAS_HOBBY", "WORK_AS"]

_date: datetime.datetime

field = Field(locale=Locale.EN)
numeric = Numeric()

# users generator
users = Schema(
    schema=lambda: {
        str(numeric.increment(accumulator="node_id")): {
            "type": "node",
            "id": str(numeric.increment(accumulator="same_node_id")),
            "labels": ["User"],
            "properties": {
                "name": field("full_name"),
                "born": field("timestamp", posix=False),
                "gender": rand.get_random_item(["M", "F"]),
                "OCEAN_openness": rand.get_random_item([True, False]),
                "OCEAN_conscientiousness": rand.get_random_item([True, False]),
                "OCEAN_agreeableness": rand.get_random_item([True, False]),
                "OCEAN_extraversion": rand.get_random_item([True, False]),
                "OCEAN_neuroticism": rand.get_random_item([True, False]),
                "_deleted": False,
            },
        }
    }
)

# random entities generator
entities = Schema(
    schema=lambda: {
        str(numeric.increment(accumulator="node_id")): {
            "type": "node",
            "id": str(numeric.increment(accumulator="same_node_id")),
            "labels": rand.get_random_item(NODE_LABELS),
            "properties": {
                "name": "".join(
                    [fabulist.get_word("adj", "#negative"), "-", generic.payment.cvv()]
                ),
                "has": "".join(
                    [
                        rand.get_random_item(
                            [
                                fabulist.get_word("adj", "#negative"),
                                fabulist.get_word("adj", "#positive"),
                            ]
                        ),
                        " ",
                        fabulist.get_word("noun"),
                    ]
                ),
                "_deleted": False,
            },
        }
    }
)

# random relationships generator
rels = Schema(
    schema=lambda: {
        "id": numeric.increment(accumulator="rel_id"),
        "type": "relationship",
        "label": rand.get_random_item(RELATIONSHIP_LABELS)[0],
        "properties": {
            "how": rand.get_random_item([fabulist.get_word("adv")]),
            "_deleted": False,
        },
    }
)

# node properties generator
node_properties = Schema(
    schema=lambda: {fabulist.get_word("noun"): fabulist.get_word("adj")}
)
# relationship properties generator
relationship_properties = Schema(schema=lambda: {"sometimes": fabulist.get_word("adv")})


def set_date(date):
    """Initiate the global vaiable date

    Args:
      date: the desired initial date of generated data

    Returns:

    """
    global _date
    _date = date


def generate_rels(iterations: int, nodes: dict) -> list:
    """Generate random relationships using the rels schema, then add start and end nodes to it.

    Args:
      iterations: number of relationships to generate
      nodes: nodes in database

    Returns:
      generated relationships

    """
    relationships = rels.create(iterations)
    nodes_ids = [key for key in nodes]
    for relationship in relationships:
        relationship.update(
            {
                "start": {"id": rand.get_random_item(nodes_ids)},
                "end": {"id": rand.get_random_item(nodes_ids)},
            }
        )
    return relationships


def iterate_generate_1node_and_1rel(
    nodes: dict,
    relationships: list,
) -> Generator:
    """Generate one node and one relationship that links the node with others.
        Add generated node and rel to database as well as to local variables so that
        they can be used in future linking

    Args:
      nodes: nodes in database
      relationships: relationships in database

    Returns:
      modified nodes and relationships after assigning the generated ones

    """
    global _date
    while True:
        node = entities.create(iterations=1)
        node = node[0][next(iter(node[0]))]
        node["properties"].update(
            {"id": node["id"], "_creation_timestamp": _date}
        )
        nodes.update({node["id"]: node})

        rel = generate_rels(iterations=1, nodes=nodes)
        rel = rel[0]
        rel["end"]["id"] = node["id"]
        rel["properties"].update({"id": rel["id"], "_creation_timestamp": _date})
        relationships.append(rel)

        graph.create_kind_node(
            kind=node["labels"][0],
            immutable_properties={
                "name": node["properties"].pop("name"),
                "id": node["properties"].pop("id"),
            },
            state_properties=node["properties"],
            create_date=node["properties"]["_creation_timestamp"],
        )
        graph.create_relationship(
            kind_a=nodes[rel["start"]["id"]]["labels"][0],
            filter_a=rel["start"],
            relationship=rel["label"],
            rel_dict=rel["properties"],
            kind_b=nodes[rel["end"]["id"]]["labels"][0],
            filter_b=rel["end"],
            create_date=rel["properties"]["_creation_timestamp"],
        )
        yield nodes, relationships


def fake_update(
    generator: Generator,
    nodes: dict,
    relationships: list,
    n_updates: int,
    interval_in_days: datetime.timedelta,
) -> Tuple[dict, list]:
    """Update the database with new relationships and entities, as well as
        updating existing ones by adding properties

    Args:
      generator: one-node-and-one-relationship generator
      nodes: nodes in database
      relationships: relationships in database
      n_updates: the number of changes we want to make on the fake-database
      updates_interval: the number of years during which the changes were made

    Returns:
      modified nodes and relationships after assigning the generated ones

    """
    global _date
    for _ in range(n_updates):
        _date += interval_in_days
        operation = rand.get_random_item(["generate", "update"])
        if operation == "update":
            to_update = rand.get_random_item(["nodes", "rels"])
            if to_update == "nodes":
                node_id = rand.get_random_item(nodes)
                new_properties = node_properties.create(iterations=randint(1, 3))
                properties_dict = {}
                for item in new_properties:
                    properties_dict.update(item)
                graph.update_node(
                    nodes[node_id]["labels"][0],
                    properties_dict,
                    {"id": node_id},
                    _date,
                )
            else:
                rel = rand.get_random_item(relationships)
                new_property = relationship_properties.create(iterations=1)[0]
                graph.update_relationship(
                    relationship=rel["label"],
                    updates=new_property,
                    change_date=_date,
                    kind_a=nodes[rel["start"]["id"]]["labels"][0],
                    kind_b=nodes[rel["end"]["id"]]["labels"][0],
                    filter_a={"id": rel["start"]["id"]},
                    filter_b={"id": rel["end"]["id"]},
                )
        elif operation == "generate":
            nodes, relationships = next(generator)
    return nodes, relationships


def generate_specific_amount_of_data(
    num_users, num_entities, num_relationships, interval_in_days
) -> Tuple[dict, list]:
    """Generate new entities and relationships and add them to database

    Args:
      num_users: number of users to generate
      num_entities: number of random entities to generate
      num_relationships: number of random relationships to generate
      interval_in_days: the period, after which the next change happens

    Returns:
      generated nodes and relationships

    """
    global _date

    some_users = users.create(iterations=num_users)
    some_entities = entities.create(iterations=num_entities)
    nodes = some_users + some_entities
    nodes_dict = {}
    for item in nodes:
        nodes_dict.update(**item)
    relationships = generate_rels(num_relationships, nodes_dict)

    for node in nodes:
        node = node[next(iter(node))]
        node["properties"].update(
            {"id": node["id"], "_creation_timestamp": _date}
        )
        graph.create_kind_node(
            kind=node["labels"][0],
            immutable_properties={
                "name": node["properties"].pop("name"),
                "id": node["properties"].pop("id"),
            },
            state_properties=node["properties"],
            create_date=node["properties"]["_creation_timestamp"],
        )
    for rel in relationships:
        _date += interval_in_days
        rel["properties"].update({"id": rel["id"], "_creation_timestamp": _date})
        graph.create_relationship(
            nodes_dict[rel["start"]["id"]]["labels"][0],
            rel["start"],
            rel["label"],
            rel["properties"],
            nodes_dict[rel["end"]["id"]]["labels"][0],
            rel["end"],
            create_date=rel["properties"]["_creation_timestamp"],
        )
    return nodes_dict, relationships
