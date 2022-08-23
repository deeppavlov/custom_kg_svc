from datetime import datetime
import logging
from deeppavlov_kg import KnowledgeGraph
from deeppavlov_kg.connector import connect_to_zu

FROM_NODE_ORDER_IN_RELATIONSHIP_RESULT = 0
HAS_STATE_RELATIONSHIP_ORDER_IN_RELATIONSHIP_RESULT = 1
STATE_NODE_ORDER_IN_RELATIONSHIP_RESULT = 2
RELATIONSHIP_ORDER_IN_RELATIONSHIP_RESULT = 3
TO_NODE_ORDER_IN_RELATIONSHIP_RESULT = 4

TEST_USER_ENTITIES = [
    {
        "Id": "1",
        "properties": {
            "name": "Jack Ryan",
            "born": datetime.strptime("1980-06-30", "%Y-%m-%d"),
            "OCEAN_openness": True,
            "OCEAN_conscientiousness": True,
            "OCEAN_agreeableness": True,
            "OCEAN_neuroticism": False,
            "OCEAN_extraversion": False,
        },
    },
    {
        "Id": "2",
        "properties": {
            "name": "Sandy Bates",
            "born": datetime.strptime("1998-04-10", "%Y-%m-%d"),
            "OCEAN_openness": True,
            "OCEAN_conscientiousness": False,
            "OCEAN_agreeableness": False,
            "OCEAN_neuroticism": True,
            "OCEAN_extraversion": True,
        },
    },
]

TEST_BOT_ENTITIES = [
    {"Id": "3", "properties": {"name": "ChatBot", "born": "2020-03-02"}}
]

TEST_UNIVERSITY_ENTITIES = [{"Id": "3a5", "properties": {"name": "Oxford"}}]

TEST_INTEREST_ENTITIES = [
    {"Id": "4", "properties": {"name": "Theater"}},
    {"Id": "5", "properties": {"name": "Artificial Intelligence"}},
    {"Id": "6", "properties": {"name": "Sport"}},
    {"Id": "7", "properties": {"name": "Mindfulness"}},
    {"Id": "8", "properties": {"name": "Medicine"}},
]

TEST_HABIT_ENTITIES = [
    {"Id": "9", "properties": {"name": "Reading", "label": "Good"}},
    {"Id": "10", "properties": {"name": "Yoga", "label": "Good"}},
    {"Id": "11", "properties": {"name": "Alcohol", "label": "Bad"}},
    {"Id": "12", "properties": {"name": "Smoking", "label": "Bad"}},
    {"Id": "13", "properties": {"name": "Dancing"}},
]

TEST_DISEASE_ENTITIES = [{"Id": "14", "properties": {"name": "Cancer"}}]

TEST_ENTITIES = {
    "User": TEST_USER_ENTITIES,
    "Bot": TEST_BOT_ENTITIES,
    "University": TEST_UNIVERSITY_ENTITIES,
    "Interest": TEST_INTEREST_ENTITIES,
    "Habit": TEST_HABIT_ENTITIES,
    "Disease": TEST_DISEASE_ENTITIES,
}

TEST_MATCHES = [
    (
        "3",
        "TALKED_WITH",
        {"on": datetime.strptime("2022-01-20", "%Y-%m-%d")},
        "2",
    ),
    ("3", "TALKED_WITH", {}, "1"),
    (
        "2",
        "KEEPS_UP",
        {"since": "March"},
        "12",
    ),
    ("2", "KEEPS_UP", {}, "9"),
    ("2", "KEEPS_UP", {}, "13"),
    ("1", "KEEPS_UP", {}, "13"),
    ("1", "STUDY", {}, "3a5"),
    ("1", "LIKES", {}, "7"),
    ("1", "DISLIKES", {}, "4"),
    ("12", "CAUSES", {}, "14"),
    ("8", "CURES", {}, "14"),
    ("10", "RELATED_TO", {}, "7"),
]


def populate(graph: KnowledgeGraph, drop=True):
    if drop:
        graph.drop_database()

    for kind, entities in TEST_ENTITIES.items():
        for entity_dict in entities:
            types = [type(value) for value in entity_dict["properties"].values()]
            graph.ontology.create_entity_kind(
                kind,
                kind_properties=list(entity_dict["properties"].keys()),
                kind_property_types=types,
            )
            graph.create_entity(
                kind,
                entity_dict["Id"],
                list(entity_dict["properties"].keys()),
                list(entity_dict["properties"].values()),
            )

    for id_a, rel, rel_dict, id_b in TEST_MATCHES:
        a_node = graph.get_entity_by_id(id_a)
        kind_a = next(iter(a_node.labels))

        b_node = graph.get_entity_by_id(id_b)
        kind_b = next(iter(b_node.labels))

        types = [type(value) for value in rel_dict.values()]
        graph.ontology.create_relationship_kind(
            rel, kind_a, kind_b, list(rel_dict.keys()), types
        )
        graph.create_relationship(
            id_a, rel, id_b, list(rel_dict.keys()), list(rel_dict.values())
        )


def search(graph: KnowledgeGraph):
    print("Search nodes:")
    habits = graph.search_for_entities("Habit")
    bad_habits = graph.search_for_entities("Habit", {"label": "Bad"})
    for key, value in {"habits": habits, "bad_habits": bad_habits}.items():
        print("\n", key)
        for habit in value:
            print(graph.get_current_state(habit[0].get("Id")).get("name"))

    print("Search relationships:")
    habits = graph.search_relationships(
        "KEEPS_UP", search_all_states=True
    )  # relationships of all time
    habits_since_march = graph.search_relationships("KEEPS_UP", {"since": "March"})

    # get sandy's habits
    sandy_id = [
        sandy[0].get("Id")
        for sandy in graph.search_for_entities("User", {"name": "Sandy Bates"})
    ][0]
    sandy_habits = graph.search_relationships("KEEPS_UP", id_a=sandy_id)

    # get sandy's bad habits
    sandy_bad_habits = []
    bad_habits_ids = [habit[0].get("Id") for habit in bad_habits]
    for habit in sandy_habits:
        if habit[TO_NODE_ORDER_IN_RELATIONSHIP_RESULT].get("Id") in bad_habits_ids:
            sandy_bad_habits.append(habit)

    # pretty print script
    for key, value in {
        "What relationships of kind KEEPS_UP are there?": habits,
        "What relationships of kind KEEPS_UP are there with property since:March?": habits_since_march,
        "What habits does sandy have?": sandy_habits,
        "What bad habits does sandy have?": sandy_bad_habits,
    }.items():
        print("\n", key)
        printed_relationships = []
        for habit in value:
            user_id = habit[FROM_NODE_ORDER_IN_RELATIONSHIP_RESULT].get("Id")
            relationship_kind = habit[RELATIONSHIP_ORDER_IN_RELATIONSHIP_RESULT].type
            habit_id = habit[TO_NODE_ORDER_IN_RELATIONSHIP_RESULT].get("Id")
            if (user_id, relationship_kind, habit_id) not in printed_relationships:
                print(
                    graph.get_current_state(user_id).get("name"),
                    relationship_kind,
                    dict(habit[RELATIONSHIP_ORDER_IN_RELATIONSHIP_RESULT].items()),
                    graph.get_current_state(habit_id).get("name"),
                )
            printed_relationships.append((user_id, relationship_kind, habit_id))

    # complex query
    print("\nWhat were Jack's habits when he was at university?")
    study_rels = graph.search_relationships(
        "STUDY", id_a="1", id_b="3a5", search_all_states=True
    )
    state_ids_when_he_was_at_university = set()
    for rel in study_rels:
        state_id = rel[STATE_NODE_ORDER_IN_RELATIONSHIP_RESULT].id
        state_ids_when_he_was_at_university.add(state_id)
    habits = []
    keeps_up_rels = graph.search_relationships(
        "KEEPS_UP", id_a="1", kind_b="Habit", search_all_states=True
    )
    for rel in keeps_up_rels:
        state_id = rel[STATE_NODE_ORDER_IN_RELATIONSHIP_RESULT].id
        if state_id in state_ids_when_he_was_at_university:
            habit = rel[TO_NODE_ORDER_IN_RELATIONSHIP_RESULT]
            if habit.id not in [item.id for item in habits]:
                habits.append(habit)
    for habit in habits:
        print(graph.get_current_state(habit.get("Id")).get("name"))


def update(graph: KnowledgeGraph):
    # Update Jack's properties
    graph.ontology.create_entity_kind_properties("User", ["height"], [int])
    jack_id = [
        jack[0].get("Id")
        for jack in graph.search_for_entities("User", {"name": "Jack Ryan"})
    ][0]
    graph.create_or_update_property_of_entity(
        id_=jack_id,
        property_kind="height",
        property_value=175,
    )

    # Update all users properties
    graph.ontology.create_entity_kind_properties("User", ["country"])
    users_ids = [user[0].get("Id") for user in graph.search_for_entities("User")]
    graph.create_or_update_properties_of_entities(
        list_of_ids=users_ids,
        list_of_property_kinds=["country"],
        list_of_property_values=["Russia"],
    )

    theater_id = [
        user[0].get("Id")
        for user in graph.search_for_entities("Interest", {"name": "Theater"})
    ][0]
    graph.ontology.create_relationship_kind_properties(
        "User", "DISLIKES", "Interest", ["every"]
    )
    graph.update_relationship(
        "DISLIKES",
        updated_property_kinds=["every"],
        updated_property_values=["Friday"],
        id_a=jack_id,
        id_b=theater_id,
    )

    uni_id = [
        entity[0].get("Id")
        for entity in graph.search_for_entities("University", {"name": "Oxford"})
    ][0]
    dancing_id = [
        entity[0].get("Id")
        for entity in graph.search_for_entities("Habit", {"name": "Dancing"})
    ][0]
    yoga_id = [
        entity[0].get("Id")
        for entity in graph.search_for_entities("Habit", {"name": "Yoga"})
    ][0]
    graph.remove_relationship("STUDY", jack_id, uni_id)
    graph.remove_relationship("KEEPS_UP", jack_id, dancing_id)
    graph.create_relationship(jack_id, "KEEPS_UP", yoga_id)


def delete(graph: KnowledgeGraph):
    smoking_id = [
        item[0].get("Id")
        for item in graph.search_for_entities("Habit", {"name": "Smoking"})
    ][0]
    sandy_id = [
        sandy[0].get("Id")
        for sandy in graph.search_for_entities("User", {"name": "Sandy Bates"})
    ][0]
    graph.remove_relationship(
        "KEEPS_UP",
        id_a=sandy_id,
        id_b=smoking_id,
    )
    graph.remove_entity(smoking_id)


def populate_from_aof_file(graph: KnowledgeGraph, aof_file_path: str):
    connect_to_zu.generate_from_aof(graph, aof_file_path)


def connect_to_zet(graph: KnowledgeGraph, redis_project_port: str):
    logging.basicConfig(level=logging.INFO)
    connect_to_zu.connect_to_redis(graph, int(redis_project_port))


def run_all(graph: KnowledgeGraph, drop_when_populating: bool = False):
    populate(graph, drop_when_populating)
    update(graph)
    search(graph)
    delete(graph)


if __name__ == "__main__":
    kg = KnowledgeGraph(
        "bolt://neo4j:neo4j@localhost:7687",
        ontology_kinds_hierarchy_path="deeppavlov_kg/database/ontology_kinds_hierarchy.pickle",
        ontology_data_model_path="deeppavlov_kg/database/ontology_data_model.json",
        db_ids_file_path="deeppavlov_kg/database/db_ids.txt",
    )

    populate(kg, drop=True)
    update(kg)
    search(kg)
    delete(kg)
