from airflow.decorators import dag,task
from datetime import datetime

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

@dag(
    start_date=datetime(2025,8,8),
    schedule='@daily',
    catchup=False,
    tags=['movie']

)
def movie():
    load_movie_to_snowflake= aql.load_file(
        task_id='load_movie_to_snowflake',
        input_file=File(path='https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv'),
        output_table=Table(
            name='movie',
            conn_id= 'snowflake'
        )
    )

movie()

# def classic_etl_dag():
#     load_data = S3ToSnowflakeOperator(
#         task_id='load_homes_data',
#         snowflake_conn_id=SNOWFLAKE_CONN_ID,
#         s3_keys=[S3_FILE_PATH + '/homes.csv'],
#         table=SNOWFLAKE_SAMPLE_TABLE,
#         schema=SNOWFLAKE_SCHEMA,
#         stage=SNOWFLAKE_STAGE,
#         file_format="(type = 'CSV',field_delimiter = ',')",
#     )