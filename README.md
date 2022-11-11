Knowledge graph API is designed to provide an easy interface to CRUD data to databases like TerminusDB and Neo4j in the runtime.
# Installation

Here is a step by step guide to install Custom KG. It will get you to a point of having 
a database up and running *in one of three ways* and the API ready to deal with it.

Clone the repository somewhere on your disk and enter the repository:
```
git clone https://github.com/deeppavlov/custom_kg_svc.git
cd custom_kg_svc
```

Install the dependencies using *pip*:
```
pip install -e .
```

## Way 1: For using Neo4j as a database:

Run the docker container inside custom_kg_svc to deploy neo4j database:

```
docker-compose up
```
Now as the neo4j database is up and running on http://localhost:7474/ you can connect to it using the API:

```python
NEO4J_BOLT_URL = "bolt://neo4j:neo4j@localhost:7687"
ONTOLOGY_KINDS_HIERARCHY_PATH = "deeppavlov_kg/database/ontology_kinds_hierarchy.pickle"
ONTOLOGY_DATA_MODEL_PATH = "deeppavlov_kg/database/ontology_data_model.json"
DB_IDS_FILE_PATH = "deeppavlov_kg/database/db_ids.txt"

neo_kg = Neo4jKnowledgeGraph(
        neo4j_bolt_url=NEO4J_BOLT_URL,
        ontology_kinds_hierarchy_path=ONTOLOGY_KINDS_HIERARCHY_PATH,
        ontology_data_model_path=ONTOLOGY_DATA_MODEL_PATH,
        db_ids_file_path=DB_IDS_FILE_PATH,
    )
```

## Way 2: For using local TerminusDB database:

[Clone the TerminusDB bootstrap repository](https://terminusdb.com/docs/get-started/install/install-as-docker-container#clone-the-terminusdb-bootstrap) and run the docker *terminusdb-container* inside. Now, your local database is ready on http://localhost:6363/ you can enter the dashboard using default username and password *(admin, root)*, respectively. To connect to it using the API you do:

```python
DB = "example_db"
TEAM ="admin"
terminus_kg = TerminusdbKnowledgeGraph(team=TEAM, db_name=DB, local=True)
```
## Way 3: For using cloud TerminusDB database:

[Create an account on TerminusX cloud](https://dashboard.terminusdb.com/), select a team, and generate a personal access token in your profile page and save it somewhere on your disk. Then, export the token in *bash* as environment variable:

```
export TERMINUSDB_ACCESS_TOKEN="YOUR_TOKEN"
```
Now, connect to the database on the cloud like so:

```python
terminus_kg = TerminusdbKnowledgeGraph(team=TEAM_FROM_CLOUD, db_name=DB)
```
## Mocks and tests
Run mocking test to populate neo4j ontology and knowledge graph
```
docker-compose up
python deeppavlov_kg/mocks/mock_base.py --neo4j
```

Run mocking test to populate neo4j ontology and knowledge graph
* Run local terminusdb docker first
* Run
    ```
    python deeppavlov_kg/mocks/mock_base.py --terminusdb
    ```
Use *--neo4j* and *--terminusdb* together to populate both in one run.

Or import mocks directly in your code

```python
from deeppavlov_kg import Neo4jKnowledgeGraph, TerminusdbKnowledgeGraph, mocks

neo4j_graph = Neo4jKnowledgeGraph(
    "bolt://neo4j:neo4j@localhost:7687",
    ontology_kinds_hierarchy_path="deeppavlov_kg/database/ontology_kinds_hierarchy.pickle",
    ontology_data_model_path="deeppavlov_kg/database/ontology_data_model.json",
    db_ids_file_path="deeppavlov_kg/database/db_ids.txt"
)
terminusdb_graph = TerminusdbKnowledgeGraph(team="admin", db_name="example_db", local=True)

# run all mocks
mocks.populate_neo4j(neo4j_graph)
mocks.populate_terminusdb(terminusdb_graph)

```
