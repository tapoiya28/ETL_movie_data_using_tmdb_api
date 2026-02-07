import sys
from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.insert(0, '/opt/airflow')
from ingest_api.api_request import extract_genres, extract_popular_movie, extract_countries, extract_languages, extract_companies

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 30
}

with DAG(
    dag_id='extracting_api-static-data-v1',
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
        op_args=[1,250],
        dag=dag
    )

    transformation_task = DockerOperator(
        task_id='dbt_transformation',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run --project-dir /usr/app/my_project',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source='D:\pj\movie_pipeline\dbt',
                target='/usr/app',
                type='bind'
            ),
            Mount(
                source='D:\pj\movie_pipeline\dbt\profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind'
            )
        ],
        network_mode="movie_pipeline_movie-network",
        docker_url='unix://var/run/docker.sock',
        auto_remove='force',
        dag=dag
    )

    [extract_genre, extract_country, extract_language] >> extract_movie >> transformation_task