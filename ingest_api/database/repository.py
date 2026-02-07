from ingest_api.database.connection import DatabaseConnection
from abc import ABC, abstractmethod

class BaseRepository(ABC):

    def __init__(self, db: DatabaseConnection, table_name: str, schema: str = 'raw'):
        self.db = db
        self.table_name = table_name
        self.schema = schema

    @abstractmethod
    def get_existing_ids(self) -> set:
        pass

    @abstractmethod
    def save(self, data):
        pass

