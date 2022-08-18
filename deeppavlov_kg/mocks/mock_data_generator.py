#!/usr/bin/env python3

import datetime
from deeppavlov_kg.mocks.generate_data import (
    graph,
    fake_update,
    generic,
    iterate_generate_1node_and_1rel,
    generate_specific_amount_of_data,
    set_date,
)

NUM_USERS = 1
NUM_ENTITIES = 3
NUM_RELATIONSHIPS = 4
UPDATES_INTERVAL = 5
NUM_UPDATES = 10
INTERVAL_IN_DAYS = datetime.timedelta(1 / (NUM_UPDATES / UPDATES_INTERVAL / 365))
INITIAL_DATE = generic.datetime.datetime()


def test_generate_specific_amount_of_data():
    """Adds data manually by specifying number of relationships, users, and other entites"""
    return generate_specific_amount_of_data(
        NUM_USERS, NUM_ENTITIES, NUM_RELATIONSHIPS, INTERVAL_IN_DAYS
    )


def test_generate_nodes_one_by_one(generator, num_nodes):
    """Generates a random node and a random relationships that connects the node with another random
       one for each iteration.
    """
    nodes, relationships = None, None
    for _ in range(num_nodes):
        nodes, relationships = next(generator)
    return nodes, relationships


def test_update_generated_data(generator, nodes, relationships):
    return fake_update(
        generator, nodes, relationships, NUM_UPDATES, INTERVAL_IN_DAYS
    )


set_date(INITIAL_DATE)

some_nodes = {}
some_relationships = []
solo_generator = iterate_generate_1node_and_1rel(
    some_nodes, some_relationships
)

graph.drop_database()
some_nodes, some_relationships = test_generate_nodes_one_by_one(
    solo_generator, NUM_ENTITIES
)
# graph.drop_database()
# some_nodes, some_relationships = test_generate_specific_amount_of_data()

test_update_generated_data(solo_generator, some_nodes, some_relationships)
