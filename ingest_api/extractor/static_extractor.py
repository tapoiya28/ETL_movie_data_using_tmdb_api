from ingest_api.database.repository.repository import BaseRepository
from ingest_api.clients.base_client import BaseAPIClient
from ingest_api.extractor.base_extractor import BaseExtractor

class StaticExtractor(BaseExtractor):
    
    def __init__(self, client: BaseAPIClient, repository: BaseRepository, type: str):
        super().__init__(client, repository)
        self.type=type # genre, language or country

    def _extract_static_data(self, fetch_fn, data_key=None):
        response = fetch_fn()
        if not response:
            return
        return response.get(data_key) if data_key and isinstance(response, dict) else response

    def extract(self):
        if self.type.lower() == 'movie_genre':
            return self._extract_static_data(
                fetch_fn=self.client.get_movie_genres, 
                data_key='genres'
            )
        elif self.type.lower() == 'language':
            return self._extract_static_data(
                fetch_fn=self.client.get_languages
            )
        elif self.type.lower() == 'country':
            return self._extract_static_data(
                fetch_fn=self.client.get_countries
            )

    def transform(self, raw_companies: list[dict]):
        pass

    def load(self, data):        
        self.repository.save(data)
