from ingest_api.database.repository import BaseRepository
from ingest_api.clients.base_client import BaseAPIClient
from ingest_api.extractor.base_extractor import BaseExtractor

class StaticExtractor(BaseExtractor):
    
    def __init__(self, client: BaseAPIClient, repository: BaseRepository, type):
        super().__init__(client, repository)
        self.type=type # genre, language or country

    def _extract_static_data(self, fetch_fn, filename, table_name, columns, data_key=None):
        response = fetch_fn()
        if not response:
            return
        return response.get(data_key) if data_key and isinstance(response, dict) else response

    def extract(self):
        if lower(self.type) == 'movie_genre':
            return self._extract_static_data(
                fetch_fn=self.client.get_movie_genres(), 
                filename=f'csv/{self.type}.csv', 
                table_name=f'raw_{self.type}_data',
                columns={'id': 'Int64', 'name': 'string'},
                data_key='genres'
            )
        elif lower(self.type) == 'language':
            return self._extract_static_data(
                fetch_fn=self.client.get_languages(), 
                filename=f'csv/{self.type}.csv', 
                table_name=f'raw_{self.type}_data',
                columns={'iso_639_1': 'string', 'english_name': 'string', 'name': 'string'}
            )
        elif lower(self.type) == 'country':
            return self._extract_static_data(
                fetch_fn=self.client.get_countries(), 
                filename=f'csv/{self.type}.csv', 
                table_name=f'raw_{self.type}_data',
                columns={'iso_3166_1': 'string', 'english_name': 'string', 'native_name': 'string'}
            )

    def transform(self, raw_companies: list[dict]):
        pass

    def load(self, data):        
        self.repository.save(data)
