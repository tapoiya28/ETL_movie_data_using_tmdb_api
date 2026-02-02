from psycopg2 import cursor
import psycopg2
import psycopg2
import sqlalchemy import create_engine

def connect_to_db(host='localhost', port='5432', dbname='movie_pipeline', user='root', password='root'):
    try:
        print("connecting to database")
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=pass
        )
        return conn
    except psycopg2.errors as e:
        print(f"connection failed: {e}")
        raise

def create_table(conn):
    print("Creating table")
    try:
        cursor = conn.cursor()
        cursor.execute(
        """
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.raw_movie_data (
            adult TEXT,
            id BIGINT,
            original_language CHAR(2),
            original_title TEXT,
            overview TEXT,
            popularity NUMERIC,
            production_country CHAR(2),
            release_date TIMESTAMP,
            revenue BIGINT,
            runtime INTEGER,
            spoken_language CHAR(2),
            status TEXT,
            tagline TEXT,
            title TEXT,
            vote_average NUMERIC,
            vote_count INTEGER
        );

        CREATE TABLE IF NOT EXISTS raw.raw_movie_genre (
            genre_id INTEGER,
            movie_id INTEGER
        );
        """
        )
    except psycopg2.errors as e:
        print(f"connection failed: {e}")
        raise

def insert_record(conn, data):
    print("inserting data...")
    try:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO TABLE raw.raw_movie_data (
                adult, id, original_language, original_title, overview, popularity, production_country, 
                release_date, revenue, runtime, spoken_language, status, tagline, title, vote_average, vote_count 
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        )
    except psycopg2.errors as e:
        print(f"connection failed: {e}")
        raise