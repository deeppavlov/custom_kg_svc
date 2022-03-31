from pydantic import BaseSettings


class OntologySettings(BaseSettings):
    neo4j_bolt_url: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # env_nested_delimiter = "__"
