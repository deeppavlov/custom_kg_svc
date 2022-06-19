import json

def read_aof(file_name):
    with open(file_name, encoding="utf-8") as file:
        lines = file.readlines()
    nodes =[]
    for line in lines:
        if line[0] == "{":
            nodes.append(json.loads(line[:-1]))
    return nodes


def read_all_redis_keys(r_db):
    keys = [key.decode("utf-8") for key in r_db.keys()]
    keys = sorted(
            keys, key=lambda k: k.split(".")[-2]
        )
    return keys


def read_redis_value(key, r_db):
    value = r_db.mget(key).pop()
    if value:
        value = json.loads(value.decode("utf-8"))
    return value
