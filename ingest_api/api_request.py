from sqlalchemy import create_engine

import requests
import csv
import pandas as pd

API_KEY = '700952bb2b594cfeb52434c089230b84'
BASE_URL = 'https://api.themoviedb.org/3'

engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5433/movie_pipeline")

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
    endpoint = f"{BASE_URL}/genre/movie/list"
    params = {
        "api_key": API_KEY,
        "language": "en-US"
    }

    try:
        response = requests.get(endpoint, params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None

# def fetch_movie_reviews():
#     endpoint = ''
#     params = {
#         "api_key": API_KEY,
#         "language": "en-US"
#     }

#     try:
#         response = requests.get(endpoint, params)
#         response.raise_for_status()
#         data = response.json()
#         return data
#     except requests.exceptions.RequestException as e:
#         print(f"Error occurred: {e}")
#         return None

def fetch_languages():
    endpoint = f'{BASE_URL}/configuration/languages'
    params = {
        "api_key": API_KEY,
        "language": "en-US"
    }

    try:
        response = requests.get(endpoint, params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")
        return None

#-----------------------------------------------------------------

# def fetch_general(endpoint):
#     params = {
#         "api_key": API_KEY,
#         "language": "en-US"
#     }

#     try:
#         response = requests.get(endpoint, params)
#         response.raise_for_status()
#         data = response.json()
#         return data
#     except requests.exceptions.RequestException as e:
#         print(f"Error occurred: {e}")
#         return None

# def fetch_detail(endpoint, id=None):
#     params = {
#         "api_key": API_KEY,
#         "language": "en-US"
#     }

#     try:
#         response = requests.get(endpoint, params)
#         response.raise_for_status()
#         data = response.json()
#         return data
#     except requests.exceptions.RequestException as e:
#         print(f"Error occurred: {e}")
#         return None

# def fetch_page(endpoint, page=1):
#     pass

#-----------------------------------------------------------------

# fetch genres
genre_col = {
    'id': 'Int64',
    'name': 'string'
}

def ingest_movie_genre():
    genre_response = fetch_movie_genres()
    if genre_response:
        movie_genre_df = pd.DataFrame(genre_response['genres'], columns=genre_col)
        # movie_genre_df.to_csv('movie_genre.csv', index=False)
        try:
            movie_genre_df.to_sql(name='movie_genre_data', con=engine, if_exists='append')
        except:
            print("error occurred")
            return

def extract_relations(movie):
    genres = movie.pop("genres")
    prod_comps = movie.pop("production_companies")
    spoken_lang = movie.pop("spoken_languages")

    movie_genres = [
        {'movie_id': movie['id'], 'genre_id': genre['id']}
        for genre in genres
    ]

    movie_comps = [
        {'movie_id': movie['id'], 'company_id': comp['id']}
        for comp in prod_comps
    ] 

    movie_langs = [
        {'movie_id': movie['id'], 'language_id': lang['iso_639_1']}
        for lang in spoken_lang
    ]
    
    return movie, movie_genres, movie_comps, movie_langs


def ingest_popular_movie(pages=1):
    """
    Ingest N first page in the popular category

    page: the number of page will be ingest
    """
    cols = [
            'adult', 'belongs_to_collection', 'id', 'original_language', 'original_title', 'popularity',
            'release_date', 'title', 'budget', 'revenue', 'runtime', 'status', 'vote_average', 'vote_count'
        ]

    movies, movie_genres, movie_comps, movie_langs = [], [], [], []

    for page in range(1, pages+1):
        movie_ids = fetch_popular_movie(page=page)
        for id in movie_ids:
            movie = fetch_detail_movie(id)
            if movie:
                m, g, c, l = extract_relations(movie)
                movies.append(m)
                movie_genres.extend(g)
                movie_comps.extend(c)
                movie_langs.extend(l)
    
    movie_df = pd.DataFrame(movies, columns=cols)
    movie_df['belongs_to_collection'] = movie_df['belongs_to_collection'].notnull()

    movie_genre_df = pd.DataFrame(movie_genres)
    movie_comp_df = pd.DataFrame(movie_comps)
    movie_lang_df = pd.DataFrame(movie_langs)

    movie_df.to_sql(name='raw.raw_movie_data', con=engine, if_exists='append', index=False)
    movie_genre_df.to_sql(name='raw.raw_mov_gen_relation', con=engine, if_exists='append', index=False)
    movie_comp_df.to_sql(name='raw.raw_mov_comp_data', con=engine, if_exists='append', index=False)
    movie_lang_df.to_sql(name='raw.raw_mov_lang_data', con=engine, if_exists='append', index=False)

ingest_popular_movie(1)


            # adult = 
            # id
            # original_language
            # original_title
            # overview
            # popularity
            # production_country
            # release_date
            # budget
            # revenue
            # runtime
            # spoken_language
            # status
            # tagline
            # title
            # vote_average
            # vote_count


    

