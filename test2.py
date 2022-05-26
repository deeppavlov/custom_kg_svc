import json
from generate_data import users, entities, generate_rel, fake_update
import graph

N_USERS = 1
N_ENTITIES = 10
N_RELATIONSHIPS = 20
UPDATES_INTERVAL = 5
N_UPDATES = 100

# Generate new entities and relationships
some_users = users.create(iterations=N_USERS)
some_entities = entities.create(iterations=N_ENTITIES)
some_nodes = some_users + some_entities
nodes_dict = {}
for item in some_nodes:
    nodes_dict.update(**item)
some_relationships = generate_rel(N_RELATIONSHIPS, nodes_dict)

# Add generated data to database
for node in some_nodes:
    node = node[next(iter(node))]
    node['properties'].update({'id': node['id']})
    graph.create_kind_node(node['labels'][0], node['properties'])
for rel in some_relationships:
    rel['properties'].update({'id':rel['id']})
    graph.create_relationship(
        nodes_dict[rel['start']['id']]['labels'][0],
        rel['start'],
        rel['label'],
        rel['properties'],
        nodes_dict[rel['end']['id']]['labels'][0],
        rel['end']
    )

# Update data in database, keeping an eye on history
nodes_dict, some_relationships = fake_update(
    nodes_dict, some_relationships, updates_interval=UPDATES_INTERVAL, n_updates=N_UPDATES
)

# Dump to JSON
dic = {}
dic['nodes'] = nodes_dict
dic['relationships'] = some_relationships
with open('node&rel.json', 'w', encoding='UTF-8') as file:
    json.dump(dic, file)
