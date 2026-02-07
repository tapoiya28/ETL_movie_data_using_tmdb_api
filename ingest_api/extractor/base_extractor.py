from ingest_api.database.repository.repository import BaseRepository
from ingest_api.clients.base_client import BaseAPIClient
from abc import ABC, abstractmethod

class BaseExtractor(ABC):
    """Base extractor for etl operations"""

    def __init__(self, client: BaseAPIClient, repository: BaseRepository):
        self.client = client
        self.repository = repository

    @abstractmethod
    def extract(self, **kwargs):
        pass
    
    @abstractmethod
    def transform(self, data: dict):
        pass

    def load(self, data):
        pass