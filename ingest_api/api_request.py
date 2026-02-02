import requests
import csv

import pandas as pd

API_KEY = '700952bb2b594cfeb52434c089230b84'
BASE_URL = 'https://api.themoviedb.org/3'

def fetch_popular_movie(page=1):
    endpoint = f"{BASE_URL}/movie/popular"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "page": page
    }
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        return [movie['id'] for movie in data.get('results', [])]
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return []
def fetch_detail_movie(movie_id):
    endpoint = f"{BASE_URL}/movie/{movie_id}"
    params = {
        "api_key": API_KEY,
        "language": "en-US"
    }
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None

def fetch_movie_genres():
    pass

def fetch_movie_reviews():
    pass

response = fetch_popular_movie(1)
movie = fetch_detail_movie(response[0])
print(movie)


# bridge = pd.DataFrame(response['results'])[['genre_ids', 'id']].explode('genre_ids').to_dict('records')
# print(bridge)


# bridge = []
# for genre_list in df['genre_ids'].head():
#     for item in genre_list:
#         bridge.append({movie})

# df = pd.DataFrame(response.get('results', []))
# flattened_genres = df[['id', 'genre_ids']].explode('genre_ids').to_dict('records')
# print(flattened_genres)