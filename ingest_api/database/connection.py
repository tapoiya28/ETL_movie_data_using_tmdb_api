from abc import ABC, abstractmethod
from sqlalchemy import create_engine

class DatabaseConnection:
    def __init__(self, connection_string: str = None):
        self.engine = create_engine(connection_string)

    def execute(self, query: str):
        with self.engine.begin() as conn:
            return conn.execute(query)

