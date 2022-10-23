import datetime

from terminusdb_client import WOQLClient, WOQLQuery as WOQL
from terminusdb_client.woqlschema import WOQLSchema, DocumentTemplate, LexicalKey
from terminusdb_client.schema.schema import TerminusClass

from deeppavlov_kg.core.graph import Neo4jKnowledgeGraph, TerminusdbKnowledgeGraph

DB = "test9"
TEAM ="Ramimashkouk|1f65"
terminus_kg = TerminusdbKnowledgeGraph(team=TEAM, db_name=DB)

NEO4J_BOLT_URL = "bolt://neo4j:neo4j@localhost:7687"
ONTOLOGY_KINDS_HIERARCHY_PATH = "/home/rami1996/integrate2/custom_kg_svc/deeppavlov_kg/database/ontology_kinds_hierarchy.pickle"
ONTOLOGY_DATA_MODEL_PATH = "/home/rami1996/integrate2/custom_kg_svc/deeppavlov_kg/database/ontology_data_model.json"
DB_IDS_FILE_PATH = "/home/rami1996/integrate2/custom_kg_svc/deeppavlov_kg/database/db_ids.txt"
neo_kg = Neo4jKnowledgeGraph(
        neo4j_bolt_url=NEO4J_BOLT_URL,
        ontology_kinds_hierarchy_path=ONTOLOGY_KINDS_HIERARCHY_PATH,
        ontology_data_model_path=ONTOLOGY_DATA_MODEL_PATH,
        db_ids_file_path=DB_IDS_FILE_PATH,
    )

neo_kg.drop_database()

neo_kg.ontology.create_entity_kind("Human")
neo_kg.ontology.create_entity_kind("Person", parent="Human")
neo_kg.ontology.create_entity_kind("Habit")
neo_kg.ontology.create_entity_kind("interest")

neo_kg.ontology.create_property_kinds("Person", ["height", "name", "weight"], [int, str, int])
neo_kg.ontology.create_property_kinds("Habit", ["name"], [str])
neo_kg.ontology.create_relationship_kind("Person", "LIKES", "Habit")
neo_kg.ontology.create_relationship_kind("Person", "HATES", "Habit")
neo_kg.create_entity("Person", "Person/Jack", ["name", "height",], ["Jack Ryan", 180])
neo_kg.create_entity("Habit", "Habit/Sport", ["name"], ["Sport"])
neo_kg.create_entity("Person", "Person/Sandy", ["name", "height"], ["Sandy Bates", 160])
neo_kg.create_entity("Habit", "Habit/Reading", ["name"], ["Reading"])

neo_kg.create_relationship("Person/Sandy", "LIKES", "Habit/Reading")

neo_kg.create_or_update_properties_of_entities(["Person/Sandy", "Person/Jack"], ["height", "weight"], [165, 70])
neo_kg.create_or_update_properties_of_entity("Person/Sandy", ["height", "weight"], [166, 77])
neo_kg.create_or_update_property_of_entity("Person/Jack", "height", 170)

neo_kg.get_properties_of_entity("Person/Sandy")
neo_kg.get_all_entities()

ts = datetime.datetime(2022,10,30)
neo_kg.get_entities_by_date(["Person/Sandy"], ts)

# # neo_kg.ontology.delete_property_kinds("Person", ["HATE"])
# # neo_kg.ontology.delete_relationship_kind("Person", "LIKES", "Habit")
# # neo_kg.delete_property_from_entity("Habit/Reading", "name")
# # neo_kg.delete_properties_from_entity("Habit/Sandy", ["height", "weight"])

# neo_kg.delete_entity("Person/Jack")


terminus_kg.drop_database()

terminus_kg.ontology.create_entity_kind("Human")
terminus_kg.ontology.create_entity_kind("Person", parent="Human")
terminus_kg.ontology.create_entity_kind("Habit")
terminus_kg.ontology.create_entity_kind("interest")

terminus_kg.ontology.create_property_kinds("Person", ["height", "name", "weight"], [int, str, int])
terminus_kg.ontology.create_property_kinds("Habit", ["name"], [str])
terminus_kg.ontology.create_relationship_kind("Person", "LIKES", "Habit")
terminus_kg.ontology.create_relationship_kind("Person", "HATES", "Habit")
terminus_kg.create_entity("Person", "Person/Jack", ["name", "height",], ["Jack Ryan", 180])
terminus_kg.create_entity("Habit", "Habit/Sport", ["name"], ["Sport"])
terminus_kg.create_entity("Person", "Person/Sandy", ["name", "height"], ["Sandy Bates", 160])
terminus_kg.create_entity("Habit", "Habit/Reading", ["name"], ["Reading"])

terminus_kg.create_relationship("Person/Sandy", "LIKES", "Habit/Reading")

terminus_kg.create_or_update_properties_of_entities(["Person/Sandy", "Person/Jack"], ["height", "weight"], [165, 70])
terminus_kg.create_or_update_properties_of_entity("Person/Sandy", ["height", "weight"], [166, 77])
terminus_kg.create_or_update_property_of_entity("Person/Jack", "height", 170)

terminus_kg.get_properties_of_entity("Person/Sandy")
terminus_kg.get_all_entities()

ts = datetime.datetime(2022,10,30)
terminus_kg.get_entities_by_date(["Person/Sandy"], ts)

# terminus_kg.ontology.delete_property_kinds("Person", ["HATE"])
# terminus_kg.ontology.delete_relationship_kind("Person", "LIKES")
# terminus_kg.delete_property_from_entity("Habit/Reading", "name")
# terminus_kg.delete_properties_from_entity("Person/Sandy", ["height", "weight"])
# terminus_kg.delete_properties_from_entities(["Person/Sandy", "Person/Jack"], ["height", "name"])

# terminus_kg.delete_entity("Person/Jack")

print("end")
