from ingest_api.database.repository.repository import BaseRepository
from ingest_api.database.repository.movie_repository import MovieRepository
from ingest_api.database.repository.company_repository import CompanyRepository
from ingest_api.database.repository.static_repository import StaticRepository

__all__ = ['BaseRepository', 'MovieRepository', 'CompanyRepository', 'StaticRepository']
