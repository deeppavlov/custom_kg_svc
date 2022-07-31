from pydantic import BaseSettings


class OntologySettings(BaseSettings):
    neo4j_bolt_url: str
    ontology_file_path: str
    ontology_data_model_path: str
    db_ids_file_path: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # env_nested_delimiter = "__"
