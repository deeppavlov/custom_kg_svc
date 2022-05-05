from typing import Tuple
import datetime
from random import randint
from fabulist import Fabulist
from mimesis import Generic, Numeric
import mimesis.random as rand
from mimesis.locales import Locale
from mimesis.schema import Field, Schema
import graph

generic = Generic(locale=Locale.EN)
fab = Fabulist()

NODE_LABELS = [[fab.get_word("noun").capitalize()] for _ in range(10)]
RELATIONSHIP_LABELS = [[fab.get_word("verb").upper()] for _ in range(10)]


_ = Field(locale=Locale.EN)
num = Numeric()

# users generator
users = Schema(schema=lambda: {
    str(num.increment(accumulator="node_id")):{
        "type":"node",
        "id": str(num.increment(accumulator="same_node_id")),
        "labels": ["User"],
        "properties":{
            "name": _("full_name"),
            "born": _("timestamp", posix=False),
            "oceanTraits":list({
                rand.get_random_item([
                    "Openness",
                    "Conscientiousness",
                    "Agreeableness",
                    "Extraversion",
                    "Neuroticism"
                ]) for _ in range(randint(1,6))
            }),
            "_creation_timestamp": _("timestamp", posix=False),
            "_deleted": False
        }
    }
})

# random entities generator
entities = Schema(schema=lambda: {
    str(num.increment(accumulator="node_id")):{
        "type":"node",
        "id": str(num.increment(accumulator="same_node_id")),
        "labels": rand.get_random_item(NODE_LABELS),
        "properties":{
            "name": fab.get_word("adj", "#negative") +"-"+ generic.payment.cvv(),
            "has": rand.get_random_item([
                fab.get_word("adj", "#negative"),
                fab.get_word("adj", "#positive")
            ]) + " " + fab.get_word("noun"),
            "_creation_timestamp": _("timestamp", posix=False),
            "_deleted": False
        }
    }
})

# random relationships generator
rels = Schema(schema=lambda: {
    "id": num.increment(accumulator="rel_id"),
    "type":"relationship",
    "label": rand.get_random_item(RELATIONSHIP_LABELS)[0],
    "properties":{
        "how": rand.get_random_item([
            fab.get_word("adv")
        ]),
        "_creation_timestamp": _("timestamp", posix=False),
        "_deleted": False
    },
})

# node properties generator
node_properties = Schema(schema=lambda: {
    fab.get_word("noun"):fab.get_word("adj")
})
# relationship properties generator
rel_properties = Schema(schema=lambda: {
    "sometimes":fab.get_word("adv")
})


def generate_rel(iterations: int, nodes: dict) -> dict:
    """Generate random relationships using the rels schema, then add start and end nodes to it

    :params iterations: number of relationships to generate
    :params nodes: nodes in database
    :return: generated relationships
    """
    relationships = rels.create(iterations)
    nodes_ids = [k for k in nodes.keys()]
    for relationship in relationships:
        relationship.update({
            "start":{
                "id":rand.get_random_item(nodes_ids)
            },
            "end":{
                "id":rand.get_random_item(nodes_ids)
            }
        })
    return relationships


def generate_data(nodes: dict, relationships: list) -> Tuple[dict, list]:
    """Generate one node and one relationship that links the node with others.
        Add generated node and rel to database as well as to local variables so that
        they can be used in future linking

    :params nodes: nodes in database
    :params relationships: relationships in database
    :return: modified nodes and relationships after assigning the generated ones
    """
    node = entities.create(iterations=1)
    node = node[0][next(iter(node[0]))]
    node['properties'].update({'id': node['id']})
    nodes.update({node['id']: node})

    rel = generate_rel(iterations=1, nodes=nodes)
    rel = rel[0]
    rel['end']['id'] = node['id']
    rel['properties'].update({'id':rel['id']})
    relationships+= [rel]

    graph.create_kind_node(node['labels'][0], node['properties'])
    graph.create_relationship(
        nodes[rel['start']['id']]['labels'][0],
        rel['start'],
        rel['label'],
        rel['properties'],
        nodes[rel['end']['id']]['labels'][0],
        rel['end']
    )
    return nodes, relationships

def fake_update(
    nodes: dict,
    relationships:list,
    updates_interval:int,
    n_updates:int
) -> Tuple[dict, list]:
    """Update the database with new relationships and entities, as well as
        updating existing ones by adding properties

    :params nodes: nodes in database
    :params relationships: relationships in database
    :params updates_interval: the number of years during which the changes were made
    :params n_updates: the number of changes we want to make on the fake-database
    :return: modified nodes and relationships after assigning the generated ones
    """
    interval_in_days = datetime.timedelta(1/(n_updates/updates_interval/365))
    change_date = generic.datetime.timestamp()

    for _ in range(n_updates):
        operation = rand.get_random_item(['generate', 'update'])
        if operation == 'update':
            to_update = rand.get_random_item(['nodes', 'rels'])
            if to_update == 'nodes':
                node_id = rand.get_random_item(nodes)
                new_properties = node_properties.create(iterations=randint(1,3))
                properties_dict = {}
                for item in new_properties:
                    properties_dict.update(item)
                graph.update_node(
                    nodes[node_id]["labels"][0],
                    properties_dict,
                    change_date,
                    {"id": node_id},
                )
            else:
                rel = rand.get_random_item(relationships)
                new_property = rel_properties.create(iterations=1)[0]
                graph.update_relationship(
                    rel["label"],
                    new_property,
                    change_date,
                    {"id": rel['id']},
                )
        elif operation =='generate':
            nodes, relationships = generate_data(nodes, relationships)

        change_date = datetime.datetime.fromtimestamp(change_date)
        change_date = datetime.datetime.timestamp(
            change_date + interval_in_days
        )
    return nodes, relationships
