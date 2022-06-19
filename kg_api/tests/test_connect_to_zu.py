import datetime

from kg_api.connector.connect_to_zu import connect_to_redis, generate_from_aof
from kg_api.core import graph

REDIS_PROJECT_PORT = 56779

track_time = datetime.datetime.now()

generate_from_aof("appendonly.aof")
print(datetime.datetime.now() - track_time)

connect_to_redis(REDIS_PROJECT_PORT)

# graph.search_nodes("ZetUniverseKindsPeoplePerson", {"Title":"Mark"})
sa_history = graph.semantic_action_history(
    "ZetUniverseKindsPeoplePerson", { "Id":"1269c693-49fe-442e-ad45-548db96ceece"}
)

node_history_table = graph.get_node_history(
    "ZetUniverseKindsPeoplePerson", { "Id":"1269c693-49fe-442e-ad45-548db96ceece"}
)
