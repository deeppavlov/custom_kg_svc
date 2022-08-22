import datetime
import logging
from deeppavlov_kg.connector.connect_to_zu import connect_to_redis, generate_from_aof, graph

logging.basicConfig(level=logging.INFO)

REDIS_PROJECT_PORT = 63608
AOF_FILE_PATH = "appendonly.aof"
track_time = datetime.datetime.now()

logging.debug(datetime.datetime.now() - track_time)

graph.drop_database()
generate_from_aof(AOF_FILE_PATH)
connect_to_redis(REDIS_PROJECT_PORT)

semantic_action_history = graph.get_semantic_action_history("0e94e17a-4354-457b-a621-dcdf0919b43f")

print(semantic_action_history)