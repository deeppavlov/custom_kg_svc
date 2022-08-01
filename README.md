## Docker
Run docker-compose to deploy neo4j database (optional)
```
docker-compose up
```

Run test to populate graph
```
python test.py
```

## Python Package
```
pip install -e .
```

```python
from deeppavlov_kg import KnowledgeGraph

graph = KnowledgeGraph(
    "bolt://neo4j:neo4j@localhost:7687",
    ontology_file_path="ontology_file.pickle",
    ontology_data_model_path="ontology_data_model.json",
    db_ids_file_path="db_ids.txt"
)
```
