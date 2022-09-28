from terminusdb_client import WOQLClient, WOQLQuery as WOQL
from terminusdb_client.woqlschema import WOQLSchema, DocumentTemplate, LexicalKey
from terminusdb_client.schema.schema import TerminusClass

from deeppavlov_kg.core.graph_base import Neo4jKnowledgeGraph, TerminusdbKnowledgeGraph
from deeppavlov_kg.core.ontology_base import OntologyConfig, Neo4jOntologyConfig, TerminusdbOntologyConfig

DB = "test9"
TEAM ="Ramimashkouk|1f65"
kg = TerminusdbKnowledgeGraph(team=TEAM, db_name=DB)

# NEO4J_BOLT_URL = "bolt://neo4j:neo4j@localhost:7687"
# ONTOLOGY_KINDS_HIERARCHY_PATH = "/home/rami1996/integrate2/custom_kg_svc/deeppavlov_kg/database/ontology_kinds_hierarchy.pickle"
# ONTOLOGY_DATA_MODEL_PATH = "/home/rami1996/integrate2/custom_kg_svc/deeppavlov_kg/database/ontology_data_model.json"
# DB_IDS_FILE_PATH = "/home/rami1996/integrate2/custom_kg_svc/deeppavlov_kg/database/db_ids.txt"
# kg = Neo4jKnowledgeGraph(
#         neo4j_bolt_url=NEO4J_BOLT_URL,
#         ontology_kinds_hierarchy_path=ONTOLOGY_KINDS_HIERARCHY_PATH,
#         ontology_data_model_path=ONTOLOGY_DATA_MODEL_PATH,
#         db_ids_file_path=DB_IDS_FILE_PATH,
#     )


kg.drop_database()

kg.ontology.create_entity_kind("Person")
kg.ontology.create_entity_kind("Habit")
kg.ontology.create_entity_kind("interest")

kg.ontology.create_property_kinds("Person", ["height", "name"], [int, str])
kg.ontology.create_property_kinds("Habit", ["name"], [str])
kg.ontology.create_relationship_kind("Person", "LIKES", "Habit")
kg.ontology.create_relationship_kind("Person", "HATES", "Habit")

kg.create_entity("Person", "Person/Jack", ["name", "height"], ["Jack Ryan", 180])
kg.create_entity("Habit", "Habit/Sport", ["name"], ["Sport"])
kg.update_entity("Person/Jack", ["name"], ["Jay"])
kg.create_relationship("Person/Jack", "LIKES", "Habit/Sport")
print("end")
