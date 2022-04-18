from datetime import datetime

import graph


TEST_USER_ENTITIES = [
    {
        "name": "Jack Ryan",
        "born": datetime.strptime("1980-06-30", "%Y-%m-%d"),
        "oceanTraits": ["Openness", "Conscientiousness", "Agreeableness"],
    },
    {
        "name": "Sandy Bates",
        "born": "1998-04-10",
        "oceanTraits": ["Openness", "Extraversion", "Neuroticism"],
    },
]

TEST_BOT_ENTITIES = [{"name": "ChatBot", "born": "2020-03-02"}]

TEST_INTEREST_ENTITIES = [
    {"name": "Theater"},
    {"name": "Artificial Intelligence"},
    {"name": "Sport"},
    {"name": "Mindfulness"},
    {"name": "Medicine"},
]

TEST_HABIT_ENTITIES = [
    {"name": "Reading", "label": "Good"},
    {"name": "Yoga", "label": "Good"},
    {"name": "Alcohol", "label": "Bad"},
    {"name": "Smoking", "label": "Bad"},
    {"name": "Dancing"},
]

TEST_DISEASE_ENTITIES = [{"name": "Cancer"}]

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
            graph.create_kind_node(kind, entity_dict)

    for kind_a, filter_a, rel, rel_dict, kind_b, filter_b in TEST_MATCHES:
        graph.create_relationship(kind_a, filter_a, rel, rel_dict, kind_b, filter_b)


def test_search():
    print("Search nodes")
    habits = graph.search_nodes("Habit")
    bad_habits = graph.search_nodes("Habit", {"label": "Bad"})

    print("Habits:\t", graph.get_properties(habits))
    print("Bad habits:\t", graph.get_properties(bad_habits))

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
        graph.display_relationships(value)


def test_update():
    graph.update_node(
        "User", {"height": 175}, filter_node={"name": "Jack Ryan"}
    )
    graph.update_node("User", {"country": "Russia"})
    # Sandy does all her habits every Friday
    graph.update_relationship(
        "KEEPS_UP", {"every": "Friday"}, kind_a="User", filter_a={"name": "Sandy Bates"}
    )
    # Sandy started to do her habits, which are since March, as daily routine
    graph.update_relationship(
        "KEEPS_UP",
        updates={"every": "day"},
        filter_rel={"since": "March"},
        kind_a="User",
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
    graph.delete_node("User", {"name": "Jack Ryan"})


def test_lookup():
    date_of_search = datetime(year=2022,month=4,day=18,hour=1,minute=0)
    node_lookup = graph.history_lookup_node(
        'User', {}, date_of_search
    )
    node_today = graph.search_nodes(
        'User', {}
    )
    relationship_lookup = graph.history_lookup_relationship(
        "KEEPS_UP",
        date_of_search,
        {},
        kind_a="User",
        filter_a={"name": "Mark Drake"},
        kind_b="Interest",
        filter_b={"name": "Sport"},
    )
    relationship_today = graph.search_relationships(
        "KEEPS_UP",
        kind_a="User",
        filter_a={"name": "Mark Drake"},
        kind_b="Interest",
        filter_b={"name": "Sport"},
    )
    print(date_of_search,'\t', node_lookup)
    print('Today', end='\t')
    print(graph.get_properties(node_today))
    print(date_of_search, '\t', relationship_lookup)
    print('Today', end='\t')
    graph.display_relationships(relationship_today)

graph.display_ontology()
test_populate(drop=False)
test_search()
test_update()
test_lookup()
test_delete()
test_update()
