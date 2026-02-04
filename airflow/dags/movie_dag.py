import sys
from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow')
from ingest_api.api_request import extract_genres, extract_popular_movie, extract_countries, extract_languages

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 30
}

with DAG(
    dag_id='extracting_api-v1',
    default_args=default_args
) as dag:

    extract_genre = PythonOperator(
        task_id='extract_genre',
        python_callable=extract_genres,
        dag=dag
    )

    extract_language = PythonOperator(
        task_id='extract_language',
        python_callable=extract_languages,
        dag=dag
    )

    extract_country = PythonOperator(
        task_id='extract_country',
        python_callable=extract_countries,
        dag=dag
    )

    extract_movie = PythonOperator(
        task_id='extract_movie_data',
        python_callable=extract_popular_movie,
        op_args=[500],
        dag=dag
    )

    [extract_genre, extract_country, extract_language] >> extract_movie