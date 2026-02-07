from ingest_api.database.repository.repository import BaseRepository
from ingest_api.clients.base_client import BaseAPIClient
from ingest_api.extractor.base_extractor import BaseExtractor

class MovieExtractor(BaseExtractor):
    
    def __init__(self, client: BaseAPIClient, repository: BaseRepository):
        super().__init__(client, repository)

    def extract(self, from_page=1, to_page=1):
        existing_ids = self.repository.get_existing_ids('id')

        movies = []
        for page in range(from_page, to_page + 1):
            movie_ids = self.client.get_popular_movie(page=page)
            for id in movie_ids:
                # avoid duplicating extraction
                if id in existing_ids:
                    continue
                # fetch detail movie information
                movies.append(self.client.get_detail_movie(id))

        return self.transform(movies)

    def transform(self, raw_movies: list[dict]):
        movies, movie_genres, movie_comps, movie_langs, movie_countries = [], [], [], [], []

        for movie in raw_movies:
            m, g, c, l, ct = self._extract_relations(movie)
            movies.append(m)
            movie_genres.extend(g)
            movie_comps.extend(c)
            movie_langs.extend(l)
            movie_countries.extend(ct)

        return {
            'main': movies,
            'relations': {
                'genre_relations': movie_genres,
                'company_relations': movie_comps,
                'language_relations': movie_langs,
                'country_relations': movie_countries
            }
        }

    def load(self, data: dict):
        movies = data['main']
        genre_relations = data['relations'].get('genre_relations')
        company_relations = data['relations'].get('company_relations')
        language_relations = data['relations'].get('language_relations')
        country_relations = data['relations'].get('country_relations')

        self.repository.save(movies)
        self.repository.save_relation(genre_relations, 'genre')
        self.repository.save_relation(company_relations, 'company')
        self.repository.save_relation(language_relations, 'language')
        self.repository.save_relation(country_relations, 'country')

    def _extract_relations(self, movie):
        genres = movie.pop("genres")
        prod_comps = movie.pop("production_companies")
        spoken_lang = movie.pop("spoken_languages")
        countries = movie.pop("production_countries")

        movie_genres = [
            {'movie_id': movie['id'], 'genre_id': genre['id']}
            for genre in genres
        ]

        movie_countries = [
            {'movie_id': movie['id'], 'country_id': country['iso_3166_1']}
            for country in countries
        ]

        movie_comps = [
            {'movie_id': movie['id'], 'company_id': comp['id']}
            for comp in prod_comps
        ] 

        movie_langs = [
            {'movie_id': movie['id'], 'language_id': lang['iso_639_1']}
            for lang in spoken_lang
        ]
        return movie, movie_genres, movie_comps, movie_langs, movie_countries