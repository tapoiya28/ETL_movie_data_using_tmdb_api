from ingest_api.clients.tmdb_client import TMDBClient

from ingest_api.database.repository.company_repository import CompanyRepository
from ingest_api.database.repository.movie_repository import MovieRepository
from ingest_api.database.repository.static_repository import StaticRepository
from ingest_api.database.connection import DatabaseConnection

from ingest_api.extractor.company_extractor import CompanyExtractor
from ingest_api.extractor.movie_extractor import MovieExtractor
from ingest_api.extractor.static_extractor import StaticExtractor


def main():
    # config connection
    USER='root'
    PASSWORD='root'
    CONTAINER='pgdatabase'
    PORT='5432'
    DATABASE='movie_pipeline'

    db = DatabaseConnection(f'postgresql://{USER}:{PASSWORD}@{CONTAINER}:{PORT}/{DATABASE}')
    client = TMDBClient(
            api_key='34206c292e84023831cfd5969adba447',
            base_url='https://api.themoviedb.org/3'
            )

    # Create repos
    print('create repo')
    movie_repo = MovieRepository(db=db, table_name='raw_movie_data')
    company_repo = CompanyRepository(db=db, table_name='raw_company_data')
    genre_repo = StaticRepository(db=db, table_name='raw_movie_genre_data')
    language_repo = StaticRepository(db=db, table_name='raw_language_data')
    country_repo = StaticRepository(db=db, table_name='raw_country_data')

    print('create extractor')
    movie_extractor = MovieExtractor(client=client, repository=movie_repo)
    company_extractor = CompanyExtractor(client=client, repository=company_repo)
    genre_extractor = StaticExtractor(client=client, repository=genre_repo, type='movie_genre')
    language_extractor = StaticExtractor(client=client, repository=language_repo, type='language')
    country_extractor = StaticExtractor(client=client, repository=country_repo, type='country')
    
    print('extract genre')
    genre_data = genre_extractor.extract()
    genre_extractor.load(genre_data)

    print('extract language')
    language_data = language_extractor.extract()
    language_extractor.load(language_data)

    print('extract country')
    country_data = country_extractor.extract()
    country_extractor.load(country_data)

    print('extract movie')
    movie_data = movie_extractor.extract(from_page=1, to_page=10)
    movie_extractor.load(movie_data)

    print('extract company')
    company_data = company_extractor.extract()
    company_extractor.load(company_data)

if __name__ == "__main__":
    main()