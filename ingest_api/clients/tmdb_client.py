from ingest_api.clients.base_client import *
import requests

class TMDBClient(BaseAPIClient):

    def __init__(self, api_key: str, base_url: str):
        super().__init__(api_key, base_url)
        self.rate_limiter = 45
    
    def fetch(self, endpoint: str, params: dict = None, extra_fn: function = None):
        """Base fetch function for all API endpoint"""

        params = {
            "api_key": API_KEY,
            "language": "en-US",
        }

        if extra_params:
            params.update(extra_params)

        try:
            response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
            response.raise_for_status()
            data = response.json()
            return extra_fn(data) if extra_fn else data
        except requests.exceptions.RequestException as e:
            print(f"Error occurred: {e}")
            return []

    #FETCH PAGE
    def get_popular_movie(self, page: int = 1):
        """ Fetch all popular movies at specific page """

        return self.fetch(
            endpoint='movie/popular',
            extra_params={'page': page},
            extra_fn=lambda data: [movie['id'] for movie in data.get('results', [])]
        ) or []

    # FETCH GENERIC INFORMATION
    def get_movie_genres(self):
        return self.fetch(
            endpoint="genre/movie/list"
        )

    def get_languages(self):
        return self.fetch(
            endpoint="configuration/languages"
        )

    def get_countries(self):
        return self.fetch(
            endpoint="configuration/countries"
        )

    # FETCH DETAIL PAGE
    def get_detail_movie(self, movie_id):
        return _fetch_from_api(
            endpoint=f'movie/{movie_id}'
        )

    def get_detail_company(self, comp_id):
        return self.fetch(
            endpoint=f'company/{comp_id}'
        )