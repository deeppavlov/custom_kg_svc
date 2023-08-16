import logging
import os
import sqlite3

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

class Index:
    def __init__(self, load_path):
        self.load_path = load_path
        self.user_id = None
        self.load()
        logger.info(f"Index was loaded. Found in path {load_path}")

    def load(self) -> None:
        if not os.path.exists(self.load_path):
            os.makedirs(self.load_path)
        self.conn = sqlite3.connect(str(self.load_path / "custom_database.db"), check_same_thread=False)
        self.cur = self.conn.cursor()
        self.cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS inverted_index USING fts5(title, entity_id, num_rels "
                         "UNINDEXED, tag, user_id, tokenize = 'porter ascii');")

    def drop_index(self) -> None:
        if os.path.exists(self.load_path / "custom_database.db"):
            os.remove(self.load_path / "custom_database.db")
            logger.info("Index was cleared successfully")
            self.load()

    def add_entities(self, entity_substr_list, entity_ids_list, tags_list):
        assert self.user_id is not None, "Active user_id should be defined. Use set_active_user_id method" #TODO: think about not adding abstract inctances to index
        user_id = self.user_id.replace("/", "slash").replace("-", "hyphen")
        logger.debug("inside add_custom_entities ---")
        if self.conn is None:
            logger.debug("inside add_custom_entities --- inside if")
            if not os.path.exists(self.load_path):
                os.makedirs(self.load_path)
            self.conn = sqlite3.connect(str(self.load_path / "custom_database.db"), check_same_thread=False)
            self.cur = self.conn.cursor()
            self.cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS inverted_index USING fts5(title, entity_id, num_rels "
                             "UNINDEXED, tag, user_id, tokenize = 'porter ascii');")

        for entity_substr, entity_id, tag in zip(entity_substr_list, entity_ids_list, tags_list):
            logger.debug("inside add_custom_entities --- inside for")
            entity_id = entity_id.replace("/", "slash").replace("-", "hyphen")
            query_str = f"title:{entity_substr} AND tag:{tag} AND user_id:{user_id}"

            query = "SELECT * FROM inverted_index WHERE inverted_index MATCH ?;"
            res = self.cur.execute(query, (query_str,)).fetchall()
            logger.debug(f"the result is {res}")
            if res and res[0][3] == "name" and res[0][1] == entity_id and tag == "name":
                query = "DELETE FROM inverted_index WHERE entity_id=? AND tag=? AND user_id=?;"
                self.cur.execute(query, (entity_id, tag, user_id))
                self.cur.execute("INSERT INTO inverted_index "
                                 "VALUES (?, ?, ?, ?, ?);", (entity_substr.lower(), entity_id, 1, tag, user_id))
                self.conn.commit()
                logger.debug("deleted and posted")
            elif not res:
                self.cur.execute("INSERT INTO inverted_index "
                                 "VALUES (?, ?, ?, ?, ?);", (entity_substr.lower(), entity_id, 1, tag, user_id))
                self.conn.commit()
                logger.debug("Just posted")

    def set_active_user_id(self, user_id: str):
        self.user_id = user_id
