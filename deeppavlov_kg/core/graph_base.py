import os
from pathlib import Path
from typing import Any, Optional, List, Tuple, Union
import logging
import datetime
from neomodel import db, config, clear_neo4j_database
from neo4j import graph as neo4j_graph
from neo4j.exceptions import ClientError
from deeppavlov_kg.core.ontology import Ontology
from deeppavlov_kg.core import querymaker
from deeppavlov_kg.core.ontology_base import Neo4jOntologyConfig, TerminusdbOntologyConfig

from terminusdb_client import WOQLClient
from terminusdb_client.errors import InterfaceError

class KnowledgeGraph:
    def __init__(
        self,
        database: str = "terminusdb",
        neo4j_bolt_url: str = "",
        ontology_kinds_hierarchy_path: Union[Path, str] = "",
        ontology_data_model_path: Union[Path, str] = "",
        db_ids_file_path: Union[Path, str] = "",
        team: str = "",
        db_name: str = "",
    ):
        if database == "terminusdb":
            if not (team and db_name):
                raise ValueError("team and db_name should be provided when the chosen database is 'terminusdb'")
            endpoint = f"https://cloud.terminusdb.com/{team}/"
            self._client   = WOQLClient(endpoint)
            try:
                self._client.connect(team=team, use_token=True, db=db_name)
            except InterfaceError:
                self._client.connect(team=team, use_token=True)
                self._client.create_database(db_name)
            self.ontology = TerminusdbOntologyConfig(self._client)

        elif database == "neo4j":
            if not (
                neo4j_bolt_url
                and ontology_kinds_hierarchy_path
                and ontology_data_model_path
                and db_ids_file_path
            ):
                raise ValueError(
                    "When the chosen database is neo4j, neo4j_bolt_url, "
                    "ontology_kinds_hierarchy_path, ontology_data_model_path, "
                    "and db_ids_file_path should be provided"
                )
            config.DATABASE_URL = neo4j_bolt_url
            self.ontology = Neo4jOntologyConfig(
                ontology_kinds_hierarchy_path, ontology_data_model_path
            )
            self.db_ids_file_path = Path(db_ids_file_path)
        else:
            raise ValueError("database only could be either 'terminusdb' or 'neo4j'")

    @classmethod # try to delete from_obj! what happens ))
    def from_obj(cls, config_obj):
        return cls(
            neo4j_bolt_url=config_obj.neo4j_bolt_url,
            ontology_kinds_hierarchy_path=config_obj.ontology_kinds_hierarchy_path,
            ontology_data_model_path=config_obj.ontology_data_model_path,
            db_ids_file_path=config_obj.db_ids_file_path,
            team=config_obj.team,
            db_name=config_obj.db_name,
        )

    def drop_database(self):
        raise NotImplementedError

    def create_entity(self, kind: str, entity_id: str, property_kinds: List[str], property_values: List[str]):
        raise NotImplementedError

    def update_entity(self, entity_id: str, property_kinds: List[str], new_property_values: List[str]):
        raise NotImplementedError

    def delete_entity(self, entity_id: str):
        raise NotImplementedError

    def get_all_entities(self):
        raise NotImplementedError
    
    def get_entity(self, entity_id: str):
        raise NotImplementedError

    def create_relationship(self, id_a: str, relationship_kind: str, id_b: str):
        raise NotImplementedError

    def update_relationship(self):
        raise NotImplementedError

    def delete_relationship(self):
        raise NotImplementedError

class Neo4jKnowledgeGraph(KnowledgeGraph):
    def __init__(
        self,
        neo4j_bolt_url: str,
        ontology_kinds_hierarchy_path: Union[Path, str],
        ontology_data_model_path: Union[Path, str],
        db_ids_file_path: Union[Path, str],
    ):
        config.DATABASE_URL = neo4j_bolt_url
        self.ontology = Neo4jOntologyConfig(
            ontology_kinds_hierarchy_path, ontology_data_model_path
        )
        self.db_ids_file_path = Path(db_ids_file_path)


class TerminusdbKnowledgeGraph(KnowledgeGraph):
    def __init__(
        self,
        team: str,
        db_name: str,
    ):
        endpoint = f"https://cloud.terminusdb.com/{team}/"
        self._client   = WOQLClient(endpoint)
        try:
            self._client.connect(team=team, use_token=True, db=db_name)
        except InterfaceError:
            self._client.connect(team=team, use_token=True)
            self._client.create_database(db_name)
        self.ontology = TerminusdbOntologyConfig(self._client)

    def drop_database(self):
        DB = self._client.db
        TEAM = self._client.team
        self._client.delete_database(DB, team=TEAM)
        self._client.create_database(DB, team=TEAM)
        logging.info("Database was recreated successfully")

    def create_entity(self, kind: str, entity_id: str, property_kinds: List[str], property_values: list):
        """create an entity, or rewrite above it if it exists"""
        return self._client.insert_document({
            "@type": kind,
            "@id": entity_id,
            **dict(zip(property_kinds, property_values)),
        })

    def update_entity(self, entity_id: str, property_kinds: List[str], new_property_values: List[str]):
        entity = self.get_entity(entity_id)
        entity.update({
                **dict(zip(property_kinds, new_property_values)),
            })
        return self._client.update_document(entity)

    def delete_entity(self, entity_id: str):
        return self._client.delete_document({
            "@id": entity_id,
        })

    def get_all_entities(self):
        raise NotImplementedError

    def get_entity(self, entity_id: str):
        return self._client.get_document(entity_id)

    def create_relationship(self, id_a: str, relationship_kind: str, id_b: str):
        self.update_entity(id_a, [relationship_kind], [id_b])

    def update_relationship(self):
        raise NotImplementedError

    def delete_relationship(self):
        raise NotImplementedError