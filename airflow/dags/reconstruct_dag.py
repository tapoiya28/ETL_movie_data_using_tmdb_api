import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

sys.path.insert(0, '/opt/airflow')
from ingest_api.main import main

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='movie_pipeline_etl_v2',
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=main
    )

    transformation_task = DockerOperator(
        task_id='dbt_transformation',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run --project-dir /usr/app/my_project',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source='D:/pj/movie_pipeline/dbt',
                target='/usr/app',
                type='bind'
            ),
            Mount(
                source='D:/pj/movie_pipeline/dbt/profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind'
            )
        ],
        network_mode="movie_pipeline_movie-network",
        docker_url='unix://var/run/docker.sock',
        auto_remove='force'
    )

    extract_task >> transformation_task