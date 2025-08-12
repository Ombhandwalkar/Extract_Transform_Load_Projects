from airflow.decorators import dag,task
from datetime import datetime
from astro import sql as apl
from astro.sql.table import Table
from astro.files import File

@dag(
    start_date='2024-07-20',
    schedule='@daily',
    catchup=False,
    tags=['movie']
)

def movie():
    load_movie_to_snowflake= apl.load_file(
        task_id='load_movie_to_snowflake',
        input_file=File(
            path='https://github.com/astronomer/astro-sdk/blob/main/tests/data/imdb.csv'
        ),
        output_table=Table(
            name='movie',
            conn_id='snowflake'
        )
    )
movie()