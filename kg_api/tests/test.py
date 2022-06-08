from datetime import datetime

import kg_api.core.graph as graph


TEST_USER_ENTITIES = [
    {
        "immutable": {
            "name": "Jack Ryan",
        },
        "mutable": {
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
            "name": "Sandy Bates",
        },
        "mutable": {
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
    {"immutable": {"name": "ChatBot"}, "mutable": {"born": "2020-03-02"}}
]

TEST_INTEREST_ENTITIES = [
    {"immutable": {"name": "Theater"}, "mutable": {}},
    {"immutable": {"name": "Artificial Intelligence"}, "mutable": {}},
    {"immutable": {"name": "Sport"}, "mutable": {}},
    {"immutable": {"name": "Mindfulness"}, "mutable": {}},
    {"immutable": {"name": "Medicine"}, "mutable": {}},
]

TEST_HABIT_ENTITIES = [
    {"immutable": {"name": "Reading"}, "mutable": {"label": "Good"}},
    {"immutable": {"name": "Yoga"}, "mutable": {"label": "Good"}},
    {"immutable": {"name": "Alcohol"}, "mutable": {"label": "Bad"}},
    {"immutable": {"name": "Smoking"}, "mutable": {"label": "Bad"}},
    {"immutable": {"name": "Dancing"}, "mutable": {}},
]

TEST_DISEASE_ENTITIES = [{"immutable": {"name": "Cancer"}, "mutable": {}}]

TEST_ENTITIES = {
    "User": TEST_USER_ENTITIES,
    "Bot": TEST_BOT_ENTITIES,
    "Interest": TEST_INTEREST_ENTITIES,
    "Habit": TEST_HABIT_ENTITIES,
    "Disease": TEST_DISEASE_ENTITIES,
}

TEST_MATCHES = [
    (
        "Bot",
        {"name": "ChatBot"},
        "TALKED_WITH",
        {"on": datetime.strptime("2022-01-20", "%Y-%m-%d")},
        "User",
        {"name": "Sandy Bates"},
    ),
    ("Bot", {"name": "ChatBot"}, "TALKED_WITH", {}, "User", {"name": "Jack Ryan"}),
    (
        "User",
        {"name": "Sandy Bates"},
        "KEEPS_UP",
        {"since": "March"},
        "Habit",
        {"name": "Smoking"},
    ),
    ("User", {"name": "Sandy Bates"}, "KEEPS_UP", {}, "Habit", {"name": "Reading"}),
    ("User", {"name": "Sandy Bates"}, "KEEPS_UP", {}, "Habit", {"name": "Dancing"}),
    ("User", {"name": "Jack Ryan"}, "KEEPS_UP", {}, "Habit", {"name": "Dancing"}),
    ("User", {"name": "Jack Ryan"}, "LIKES", {}, "Interest", {"name": "Mindfulness"}),
    ("User", {"name": "Jack Ryan"}, "DISLIKES", {}, "Interest", {"name": "Theater"}),
    ("Habit", {"name": "Smoking"}, "CAUSES", {}, "Disease", {"name": "Cancer"}),
    ("Interest", {"name": "Medicine"}, "CURES", {}, "Disease", {"name": "Cancer"}),
    ("Habit", {"name": "Yoga"}, "RELATED_TO", {}, "Interest", {"name": "Mindfulness"}),
]


def test_populate(drop=True):
    if drop:
        graph.drop_database()

    for kind, entities in TEST_ENTITIES.items():
        for entity_dict in entities:
            graph.create_kind_node(
                kind, entity_dict["immutable"], entity_dict["mutable"]
            )

    for kind_a, filter_a, rel, rel_dict, kind_b, filter_b in TEST_MATCHES:
        graph.create_relationship(kind_a, filter_a, rel, rel_dict, kind_b, filter_b)


def test_search():
    print("Search nodes")
    habits = graph.search_nodes("Habit")
    bad_habits = graph.search_nodes("Habit", {"label": "Bad"})
    for key, value in {"habits": habits, "bad_habits": bad_habits}.items():
        print("\n", key)
        for habit in value:
            print(habit[0]._properties["name"])

    print("Search relationships")
    habits = graph.search_relationships("KEEPS_UP")
    habits_since_march = graph.search_relationships("KEEPS_UP", {"since": "March"})
    sandy_habits = graph.search_relationships(
        "KEEPS_UP", kind_a="User", filter_a={"name": "Sandy Bates"}
    )
    sandy_bad_habits = graph.search_relationships(
        "KEEPS_UP",
        kind_a="User",
        filter_a={"name": "Sandy Bates"},
        kind_b="Habit",
        filter_b={"label": "Bad"},
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
                habit[0]._properties["name"],
                habit[1].type,
                habit[1]._properties,
                habit[2]._properties["name"],
            )


def test_update():
    graph.update_node(
        "User", {"height": 175, "name": "Jay Ryan"}, filter_node={"name": "Jack Ryan"}
    )
    graph.update_node("User", {"country": "Russia"})
    # Sandy does all her habits every Friday
    graph.update_relationship(
        "KEEPS_UP",
        {"every": "Friday"},
        kind_a="User",
        kind_b="Habit",
        filter_a={"name": "Sandy Bates"},
    )
    # Sandy started to do her habits, which are since March, as daily routine
    graph.update_relationship(
        "KEEPS_UP",
        updates={"every": "day"},
        kind_a="User",
        kind_b="Habit",
        filter_a={"name": "Sandy Bates"},
    )
    graph.update_relationship(
        "KEEPS_UP",
        updates={"since": "February"},
        kind_a="User",
        filter_a={"name": "Sandy Bates"},
        kind_b="Habit",
        filter_b={"label": "Good"},
    )


def test_delete():
    graph.delete_relationship(
        "KEEPS_UP",
        kind_a="User",
        filter_a={"name": "Sandy Bates"},
        kind_b="Habit",
        filter_b={"name": "Dancing"},
    )
    graph.delete_node("User", {"name": "Jack Ryan"}, completely=False)


test_populate()
test_search()
test_update()
test_delete()
