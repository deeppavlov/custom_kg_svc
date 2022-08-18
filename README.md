## Docker
Run docker-compose to deploy neo4j database (optional)
```
docker-compose up
```

## Python Package
```
pip install -e .
```

```python
from deeppavlov_kg import KnowledgeGraph

graph = KnowledgeGraph(
    "bolt://neo4j:neo4j@localhost:7687",
    ontology_kinds_hierarchy_path="deeppavlov_kg/database/ontology_kinds_hierarchy.pickle",
    ontology_data_model_path="deeppavlov_kg/database/ontology_data_model.json",
    db_ids_file_path="deeppavlov_kg/database/db_ids.txt"
)
```

## Mocks and tests
Run mocking test to populate graph
```
python deeppavlov_kg/mocks/mock.py
```

Or import mocks directly in your code

```python
from deeppavlov_kg import KnowledgeGraph, mocks

graph = KnowledgeGraph(
    "bolt://neo4j:neo4j@localhost:7687",
    ontology_kinds_hierarchy_path="deeppavlov_kg/database/ontology_kinds_hierarchy.pickle",
    ontology_data_model_path="deeppavlov_kg/database/ontology_data_model.json",
    db_ids_file_path="deeppavlov_kg/database/db_ids.txt"
)

# run all mocks
mocks.run_all(graph, drop_when_populating=True)

# or run selected mocks
mocks.populate(graph, drop=True)
mocks.search(graph)
mocks.update(graph)
mocks.delete(graph)
```
