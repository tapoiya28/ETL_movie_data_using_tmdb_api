from ingest_api.database.connection import DatabaseConnection
from ingest_api.database.repository.repository import BaseRepository

import pandas as pd

class MovieRepository(BaseRepository):

    cols = [
            'adult', 'belongs_to_collection', 'id', 'original_language', 'original_title', 'popularity',
            'release_date', 'title', 'budget', 'revenue', 'runtime', 'status', 'vote_average', 'vote_count'
        ]

    def __init__(self, db: DatabaseConnection, table_name: str, schema: str = 'raw'):
        super().__init__(db, table_name, schema)

    def get_existing_ids(self, id_col: str = 'id'):
        query = f"SELECT DISTINCT {id_col} FROM {self.schema}.{self.table_name}"
        try:
            return set(pd.read_sql(query, con=self.db.engine)[f'{id_col}'].tolist())
        except Exception:
            return set()

    def save(self, data):
        df = pd.DataFrame(data, columns=self.cols)
        df.to_sql(
            schema=self.schema, 
            name=self.table_name, 
            con=self.db.engine, 
            if_exists='append', 
            index=False
        )
    
    def save_relation(self, relations: list[dict], name):
        df = pd.DataFrame(relations)
        relation_name = f'raw_mov_{name[:4]}_relation'
        df.to_sql(
            schema=self.schema, 
            name=relation_name, 
            con=self.db.engine, 
            if_exists='append', 
            index=False
        )

    