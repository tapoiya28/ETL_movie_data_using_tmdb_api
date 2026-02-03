import psycopg2
from sqlalchemy import create_engine

def connect_to_db(host='localhost', port='5432', dbname='movie_pipeline', user='root', password='root'):
    try:
        print("connecting to database")
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
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
                budget BIGINT,
                revenue BIGINT,
                runtime INTEGER,
                spoken_language CHAR(2),
                status TEXT,
                tagline TEXT,
                title TEXT,
                vote_average NUMERIC,
                vote_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS raw.raw_genre_data (
                id INTEGER,
                name TEXT
            );

            CREATE TABLE IF NOT EXISTS raw.raw_company_data (
                id INTEGER,
                name TEXT,
                origin_country CHAR(2)
            );

            CREATE TABLE IF NOT EXISTS raw.raw_language_data (
                iso_639_1 CHAR(2),
                english_name TEXT,
                name TEXT
            );

            CREATE TABLE IF NOT EXISTS raw.raw_country_data (
                iso_3166_1 CHAR(2),
                english_name TEXT,
                native_name TEXT
            );
            """
        )
        conn.commit()
    except psycopg2.errors as e:
        print(f"connection failed: {e}")
        raise

def insert_record(conn, data):
    print("inserting data...")
    try:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO raw.raw_movie_data (
                adult, id, original_language, original_title, overview, popularity, production_country, 
                release_date, revenue, runtime, spoken_language, status, tagline, title, vote_average, vote_count 
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            data
        )
        conn.commit()
    except psycopg2.errors as e:
        print(f"connection failed: {e}")
        raise


if __name__ == "__main__":
    conn = connect_to_db(port=5433)
    create_table(conn)
    conn.close()