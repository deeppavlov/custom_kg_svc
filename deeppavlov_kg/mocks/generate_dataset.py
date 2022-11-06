from rdflib import Graph
from deeppavlov_kg.core.graph import Neo4jKnowledgeGraph, TerminusdbKnowledgeGraph
import datetime
import uuid
import random

NUMBER_OF_ENTITIES = 40
NUMBER_OF_RELATIONSHIPS = 70

TTL_FILE_PATH = "deeppavlov_kg/database/data.ttl"
DB = "test"
TEAM ="Ramimashkouk|1f65"

NEO4J_BOLT_URL = "bolt://neo4j:neo4j@localhost:7687"
ONTOLOGY_KINDS_HIERARCHY_PATH = "deeppavlov_kg/database/ontology_kinds_hierarchy.pickle"
ONTOLOGY_DATA_MODEL_PATH = "deeppavlov_kg/database/ontology_data_model.json"
DB_IDS_FILE_PATH = "deeppavlov_kg/database/db_ids.txt"


from_the_very_beginning = datetime.datetime.now()


def write_list_to_file(name_of_lst, lst):
    with open(f"deeppavlov_kg/database/{name_of_lst}.txt", "w+") as file:
        for item in lst:
            file.write(item+"\n")


def read_data():
    with open(f"deeppavlov_kg/database/entities_kinds_list.txt", "r") as file:
        entities_kinds_list = file.read().splitlines()
    with open(f"deeppavlov_kg/database/entities_ids_list.txt", "r") as file:
        entities_ids_list = file.read().splitlines()
    with open(f"deeppavlov_kg/database/ids_a.txt", "r") as file:
        ids_a = file.read().splitlines()
    with open(f"deeppavlov_kg/database/relationships.txt", "r") as file:
        relationships = file.read().splitlines()
    with open(f"deeppavlov_kg/database/ids_b.txt", "r") as file:
        ids_b = file.read().splitlines()

    return entities_kinds_list, entities_ids_list, ids_a, relationships, ids_b


def read_ttl_ontology(graph):
    knows_query = """
        SELECT DISTINCT ?a ?b
        WHERE {
            ?a rdf:type owl:Class .
            ?a rdfs:subClassOf ?b .
            ?b rdf:type owl:Class .
        }
    """
    qres = graph.query(knows_query)
    kinds = ["Dessert", "Ingredient"]
    parents = [None, None]
    for kind_a, kind_b in qres:
        kind_a = kind_a.split("#")[-1]
        kind_b = kind_b.split("#")[-1]
        kinds.append(kind_a)
        parents.append(kind_b)

    # create properties
    knows_query = """
        SELECT DISTINCT ?dessert ?ingredient
        WHERE {
            ?dessert rdfs:subClassOf ?a .
            ?a owl:onProperty bakery:hasIngredient .
            ?a owl:someValuesFrom ?ingredient
        }
    """
    qres = graph.query(knows_query)
    desserts = []
    ingredients = []
    for dessert, ingredient in qres:
        dessert = dessert.split("#")[-1]
        ingredient = ingredient.split("#")[-1]
        desserts.append(dessert)
        ingredients.append(ingredient)

    return kinds, parents, desserts, ingredients


def populate_ontology_terminus(kg, kinds, parents, desserts, ingredients):    
    start_time = datetime.datetime.now()
    kg.ontology.create_entity_kinds(kinds, parents)
    dessert_ingredient_rels = [f"has_ingredient_{ingredient}" for ingredient in ingredients]
    kg.ontology.create_relationship_kinds(desserts, dessert_ingredient_rels, ingredients)
    print("Populating ontology took: ", datetime.datetime.now() - start_time)


def populate_ontology_neo4j(kg, kinds, parents, desserts, ingredients):    
    start_time = datetime.datetime.now()
    for kind_a, kind_b in zip(kinds, parents):
        kg.ontology.create_entity_kind(kind_b)
        kg.ontology.create_entity_kind(kind_a, parent=kind_b)
    elapsed_time = datetime.datetime.now() - start_time

    start_time = datetime.datetime.now()
    for dessert, ingredient in zip(desserts, ingredients):
        kg.ontology.create_relationship_kind(dessert, f"has_ingredient_{ingredient}", ingredient)
    elapsed_time += datetime.datetime.now() - start_time
    print("Populating ontology took: ", elapsed_time)


def generate_data(kinds):
    dessert_with_ing = []
    desserts = terminus_kg.ontology.get_all_entity_kinds()
    for _, dessert in desserts.items():
        for k, v in dessert.items():
            if k.startswith("has_ingredient_"):
                dessert_with_ing.append((dessert["@id"], k, v.get("@class")))

    entities = {}
    for _ in range(NUMBER_OF_ENTITIES):
        kind = random.choice(list(kinds))
        id = "/".join([kind, str(uuid.uuid4())])
        if kind not in entities:
            entities[kind] = []
        entities[kind].append(id)

    entities_kinds_list = []
    entities_ids_list = []
    for entity_kind, ids in entities.items():
        entities_ids_list += ids
        entities_kinds_list += [entity_kind]*len(ids)


    triples = []
    for _ in range(NUMBER_OF_RELATIONSHIPS):
        triple = random.choice(dessert_with_ing)
        kind_a, rel, kind_b = triple
        
        while entities.get(kind_a) is None or entities.get(kind_b) is None:
            triple = random.choice(dessert_with_ing)
            kind_a, rel, kind_b = triple

        id_a = random.choice(entities[kind_a])
        id_b = random.choice(entities[kind_b])

        avoid_endless_loop_counter = 0
        while (id_a, rel, id_b) in triples and avoid_endless_loop_counter<10:
            id_a = random.choice(entities[kind_a])
            id_b = random.choice(entities[kind_b])
            avoid_endless_loop_counter+=1
        if avoid_endless_loop_counter>=10:
            continue

        triples.append((id_a, rel, id_b))

    ids_a = [triple[0] for triple in triples]
    relationships = [triple[1] for triple in triples]
    ids_b = [triple[2] for triple in triples]

    write_list_to_file("entities_kinds_list", entities_kinds_list)
    write_list_to_file("entities_ids_list", entities_ids_list)
    write_list_to_file("ids_a", ids_a)
    write_list_to_file("relationships", relationships)
    write_list_to_file("ids_b", ids_b)

    return entities_kinds_list, entities_ids_list, ids_a, relationships, ids_b


# terminus_kg = TerminusdbKnowledgeGraph("admin", DB, True)
terminus_kg = TerminusdbKnowledgeGraph(team=TEAM, db_name=DB)

neo_kg = Neo4jKnowledgeGraph(
        neo4j_bolt_url=NEO4J_BOLT_URL,
        ontology_kinds_hierarchy_path=ONTOLOGY_KINDS_HIERARCHY_PATH,
        ontology_data_model_path=ONTOLOGY_DATA_MODEL_PATH,
        db_ids_file_path=DB_IDS_FILE_PATH,
    )

g = Graph()
g.parse(TTL_FILE_PATH, format="ttl")
kinds, parents, desserts, ingredients = read_ttl_ontology(g)


terminus_kg.drop_database()
populate_ontology_terminus(terminus_kg, kinds, parents, desserts, ingredients)

kinds_to_generate = set(kinds+parents)
kinds_to_generate.remove(None)

entities_kinds_list, entities_ids_list, ids_a, relationships, ids_b = generate_data(kinds_to_generate)
# entities_kinds_list, entities_ids_list, ids_a, relationships, ids_b = read_data()

start_time = datetime.datetime.now()
print("Number of entities: ", len(entities_ids_list))
print("Number of relationships: ", len(relationships))
terminus_kg.create_entities(entities_kinds_list, entities_ids_list)
terminus_kg.create_relationships(ids_a, relationships, ids_b)
print("Populating kg took: ", datetime.datetime.now() - start_time)



neo_kg.drop_database()
kinds.remove("Dessert")
kinds.remove("Ingredient")
parents.remove(None)
parents.remove(None)
populate_ontology_neo4j(neo_kg, kinds, parents, desserts, ingredients)

start_time = datetime.datetime.now()
for entity_kind, entity_id in zip(entities_kinds_list, entities_ids_list):
    neo_kg.create_entity(entity_kind, entity_id, [], [])
for id_a, rel, id_b in zip(ids_a, relationships, ids_b):
    try:
        neo_kg.create_relationship(id_a, rel, id_b)
    except ValueError as e:
        print(e)
        pass

print("Populating kg took: ", datetime.datetime.now() - start_time)


print(datetime.datetime.now() - from_the_very_beginning)
print("end")
