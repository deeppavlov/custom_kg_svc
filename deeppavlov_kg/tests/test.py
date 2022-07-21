from datetime import datetime

import deeppavlov_kg.core.graph as graph


TEST_USER_ENTITIES = [
    {
        "immutable": {
            "Id": "1",
        },
        "mutable": {
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
        "immutable": {
            "Id":"2",
        },
        "mutable": {
            "name": "Sandy Bates",
            "born": "1998-04-10",
            "OCEAN_openness": True,
            "OCEAN_conscientiousness": False,
            "OCEAN_agreeableness": False,
            "OCEAN_neuroticism": True,
            "OCEAN_extraversion": True,
        },
    },
]

TEST_BOT_ENTITIES = [
    {"immutable": {"Id": "3"}, "mutable": {"name": "ChatBot", "born": "2020-03-02"}}
]

TEST_INTEREST_ENTITIES = [
    {"immutable": {"Id": "4"}, "mutable": {"name": "Theater"}},
    {"immutable": {"Id": "5"}, "mutable": {"name": "Artificial Intelligence"}},
    {"immutable": {"Id": "6"}, "mutable": {"name": "Sport"}},
    {"immutable": {"Id": "7"}, "mutable": {"name": "Mindfulness"}},
    {"immutable": {"Id": "8"}, "mutable": {"name": "Medicine"}},
]

TEST_HABIT_ENTITIES = [
    {"immutable": {"Id": "9"}, "mutable": {"name": "Reading", "label": "Good"}},
    {"immutable": {"Id": "10"}, "mutable": {"name": "Yoga", "label": "Good"}},
    {"immutable": {"Id": "11"}, "mutable": {"name": "Alcohol", "label": "Bad"}},
    {"immutable": {"Id": "12"}, "mutable": {"name": "Smoking", "label": "Bad"}},
    {"immutable": {"Id": "13"}, "mutable": {"name": "Dancing"}},
]

TEST_DISEASE_ENTITIES = [{"immutable": {"Id": "14"}, "mutable": {"name": "Cancer"}}]

TEST_ENTITIES = {
    "User": TEST_USER_ENTITIES,
    "Bot": TEST_BOT_ENTITIES,
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
    ("1", "LIKES", {}, "7"),
    ("1", "DISLIKES", {}, "4"),
    ("12", "CAUSES", {}, "14"),
    ("8", "CURES", {}, "14"),
    ("10", "RELATED_TO", {}, "7"),
]


def test_populate(drop=True):
    if drop:
        graph.drop_database()

    for kind, entities in TEST_ENTITIES.items():
        for entity_dict in entities:
            graph.create_entity(
                kind, entity_dict["immutable"]["Id"], entity_dict["mutable"]
            )

    for id_a, rel, rel_dict, id_b in TEST_MATCHES:
        graph.create_relationship(id_a, rel, rel_dict,id_b)


def test_search():
    print("Search nodes")
    habits = graph.search_for_entities("Habit")
    bad_habits = graph.search_for_entities("Habit", {"label": "Bad"})
    for key, value in {"habits": habits, "bad_habits": bad_habits}.items():
        print("\n", key)
        for habit in value:
            print(graph.get_current_state(habit[0].get("Id")).get("name"))

    print("Search relationships")
    habits = graph.search_relationships("KEEPS_UP")
    habits_since_march = graph.search_relationships("KEEPS_UP", {"since": "March"})
    sandy_habits = graph.search_relationships(
        "KEEPS_UP", id_a="2"
    )
    sandy_bad_habits = graph.search_relationships(
        "KEEPS_UP",
        id_a="2",
        id_b="11",
    )
    for key, value in {
        "habits": habits,
        "habits_since_march": habits_since_march,
        "sandy_habits": sandy_habits,
        "sandy_bad_habits": sandy_bad_habits,
    }.items():
        print("\n", key)
        for habit in value:
            print(
                graph.get_current_state(habit[0].get("Id")).get("name"),
                habit[1].type,
                dict(habit[1].items()),
                graph.get_current_state(habit[2].get("Id")).get("name"),
            )


def test_update():
    graph.create_or_update_properties_of_entity(
        id_="1",
        list_of_property_kinds=["height", "name"],
        list_of_property_values= [175, "Jay Ryan"],
    )
    graph.create_or_update_properties_of_entities(
        list_of_ids=["1","2"],
        list_of_property_kinds=["country"],
        list_of_property_values=["Russia"],
    )
    # Sandy does all her habits every Friday
    graph.update_relationship(
        "KEEPS_UP",
        {"every": "Friday"},
        id_a="2",
        id_b="10",
    )
    # Sandy started to do her habits, which are since March, as daily routine
    graph.update_relationship(
        "KEEPS_UP",
        updates={"every": "day"},
        id_a="2",
        id_b="9",
    )
    graph.update_relationship(
        "KEEPS_UP",
        updates={"since": "February"},
        id_a="2",
        id_b="13",
    )


def test_delete():
    graph.delete_relationship(
        "KEEPS_UP",
        id_a="1",
        id_b="13",
    )
    graph.delete_entity("1", completely=False)


test_populate()
test_search()
test_update()
test_delete()
