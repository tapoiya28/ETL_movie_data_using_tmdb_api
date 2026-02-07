from ingest_api.database.connection import DatabaseConnection
from repository import BaseRepository

import pandas as pd

class StaticRepository(BaseRepository):

    def __init__(self, db: DatabaseConnection, table_name: str, schema: str = 'raw'):
        super().__init__(db, table_name, schema)

    def get_existing_ids(self, id_col: str = 'id'):
        query = f"SELECT DISTINCT {id_col} FROM {self.schema}.{self.table_name}"
        try:
            return set(pd.read_sql(query, con=self.db.engine)[f'{id_col}'].tolist())
        except Exception:
            return set()

    def save(self, data):
        df = pd.DataFrame(data)
        df.to_sql(
            schema=self.schema, 
            name=self.table_name, 
            con=self.db.engine, 
            if_exists='replace', 
            index=False
        )