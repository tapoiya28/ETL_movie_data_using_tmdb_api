from requests import request
from unicodedata import name
from datetime import datetime, timezone
from sqlalchemy import create_engine
import time

import requests
import os
import pandas as pd

API_KEY = '34206c292e84023831cfd5969adba447'
BASE_URL = 'https://api.themoviedb.org/3'

engine = create_engine('postgresql://root:root@pgdatabase:5432/movie_pipeline')

def get_existing_ids(table_name):
    query = f"SELECT DISTINCT id FROM raw.{table_name}"
    try:
        return set(pd.read_sql(query, con=engine)['id'].tolist())
    except Exception:
        return set()

# ----------------- FETCHING DATA -----------------------------
def _fetch_from_api(endpoint, extra_params=None, extra_fn=None):
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

def fetch_popular_movie(page=1):
    return _fetch_from_api(
        endpoint='movie/popular',
        extra_params={'page': page},
        extra_fn=lambda data: [movie['id'] for movie in data.get('results', [])]
    ) or []

def fetch_detail_movie(movie_id):
    return _fetch_from_api(
        endpoint=f'movie/{movie_id}'
    )

def fetch_movie_genres():
    return _fetch_from_api(
        endpoint="genre/movie/list"
    )

def fetch_languages():
    return _fetch_from_api(
        endpoint="configuration/languages"
    )

def fetch_countries():
    return _fetch_from_api(
        endpoint="configuration/countries"
    )

def fetch_companies(name):
    return _fetch_from_api(
        endpoint="search/company",
        extra_params={},
        extra_fn=lambda data: [comp['id'] for comp in data.get('results', [])]
    ) or []

def fetch_detail_company(comp_id):
    return _fetch_from_api(
        endpoint=f'company/{comp_id}'
    )

# ----------------- EXTRACTING DATA -----------------------------
def _extract_static_data(fetch_fn, filename, table_name, columns, data_key=None):
    response = fetch_fn()
    if not response:
        return
    
    data = response.get(data_key) if data_key and isinstance(response, dict) else response
    df = pd.DataFrame(data, columns=columns)
    
    try:
        if not os.path.exists(filename):
            df.to_csv(filename, index=False)

        with engine.begin() as conn:
            conn.execute(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE")

        df.to_sql(schema='raw', name=table_name, con=engine, if_exists='replace', index=False)
    except Exception as e:
        print(f"error occurred when extracting static data: {e}")
        return None

def extract_genres():
    return _extract_static_data(
        fetch_fn=fetch_movie_genres, 
        filename='csv/movie_genre.csv', 
        table_name='raw_movie_genre_data',
        columns={'id': 'Int64', 'name': 'string'},
        data_key='genres'
    )

def extract_languages():
    return _extract_static_data(
        fetch_fn=fetch_languages, 
        filename='csv/language.csv', 
        table_name='raw_language_data',
        columns={'iso_639_1': 'string', 'english_name': 'string', 'name': 'string'}
    )

def extract_countries():
    return _extract_static_data(
        fetch_fn=fetch_countries, 
        filename='csv/country.csv', 
        table_name='raw_country_data',
        columns={'iso_3166_1': 'string', 'english_name': 'string', 'native_name': 'string'}
    )

def extract_companies(comp_ids):
    exist_companies = get_existing_ids('raw_company_data')
    print('extracting company: ', len(comp_ids))

    cols = ['id', 'name', 'original_country', 'headquarters', 'parent_company_id']
    comps = []
    for comp_id in comp_ids:
        if comp_id in exist_companies:
            continue

        comp = fetch_detail_company(comp_id)
        # Extract parent_company_id from dict 
        if comp and isinstance(comp.get('parent_company'), dict):
            comp['parent_company_id'] = comp['parent_company'].get('id')
        else:
            comp['parent_company_id'] = None
        comp.pop('parent_company', None)  # Remove the dict field
        comps.append(comp)

    print('total_comps:', len(comps))
    comp_df = pd.DataFrame(comps, columns=cols)
    try:
        if not os.path.exists('csv/company.csv'):
            comp_df.to_csv('csv/company.csv', index=False)
            
        comp_df.to_sql(schema='raw', name='raw_company_data', con=engine, if_exists='append', index=False)
        
    except Exception as e:
        print(f"error occurred when extracting company data: {e}")

def extract_popular_movie(from_page=1, to_page=1):
    """
    extract N first page in the popular category

    page: the number of page will be extract
    """
    def extract_relations(movie):
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

    cols = [
            'adult', 'belongs_to_collection', 'id', 'original_language', 'original_title', 'popularity',
            'release_date', 'title', 'budget', 'revenue', 'runtime', 'status', 'vote_average', 'vote_count'
        ]

    # check existing movie
    exist_movie = get_existing_ids('raw_movie_data')
    print('unique movie_ids:', len(exist_movie))

    # Rate limiting tracker
    request_cnt = 0
    start_time = time.time()

    print('extracting movie')
    movies, movie_genres, movie_comps, movie_langs, movie_countries = [], [], [], [], []
    for page in range(from_page, to_page + 1):
        movie_ids = fetch_popular_movie(page=page)
        request_cnt += 1  # count the page request too
        
        for id in movie_ids:
            # avoid duplicating extraction
            if id in exist_movie:
                continue
            
            # fetch detail movie information
            movie = fetch_detail_movie(id)
            request_cnt += 1

            # Sleep only when approaching rate limit (40 requests per second)
            if request_cnt >= 44:
                elapsed = time.time() - start_time
                if elapsed < 1:
                    time.sleep(1 - elapsed)
                request_cnt = 0
                start_time = time.time()

            if movie:
                m, g, c, l, ct = extract_relations(movie)
                movies.append(m)
                movie_genres.extend(g)
                movie_comps.extend(c)
                movie_langs.extend(l)
                movie_countries.extend(ct)

    print('get unique company id')
    query = """select distinct company_id from raw.raw_mov_comp_relation"""
    lst = set(pd.read_sql(query, con=engine)['company_id'].tolist())
    # EXTRACT COMPANY HERE BC THE API DOES NOT SUPPORT FOR LIST OF COMPANIES
    comp_ids = list(set(comp.get('company_id') for comp in movie_comps).union(lst))
    extract_companies(comp_ids)

    print('to database')
    # dataframe
    movie_df = pd.DataFrame(movies, columns=cols)
    movie_df['belongs_to_collection'] = movie_df['belongs_to_collection'].notnull()
    movie_df['extracted_at'] = datetime.now(timezone.utc)

    movie_genre_df = pd.DataFrame(movie_genres)
    movie_comp_df = pd.DataFrame(movie_comps)
    movie_lang_df = pd.DataFrame(movie_langs)
    movie_ctry_df = pd.DataFrame(movie_countries)

    # load to raw schema
    movie_df.to_sql(schema='raw', name='raw_movie_data', con=engine, if_exists='append', index=False)
    movie_genre_df.to_sql(schema='raw', name='raw_mov_gen_relation', con=engine, if_exists='append', index=False)
    movie_comp_df.to_sql(schema='raw', name='raw_mov_comp_relation', con=engine, if_exists='append', index=False)
    movie_lang_df.to_sql(schema='raw', name='raw_mov_lang_relation', con=engine, if_exists='append', index=False)
    movie_ctry_df.to_sql(schema='raw', name='raw_mov_ctry_relation', con=engine, if_exists='append', index=False)
