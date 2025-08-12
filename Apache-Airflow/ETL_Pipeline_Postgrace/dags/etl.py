from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json 

# Define DAG
with DAG(
    dag_id='nasa_apod_postgres', 
    start_date=datetime(2025,8,5),
    schedule='@daily',
    catchup=False # This will not automatically schedule the task

) as dag:
    
    # Step-1 Create table if does not exists
    @task
    def create_table():
        # Initialize the PostgresHook - This will help Postgres to connect with Airflow
        postgres_hook= PostgresHook(postgres_conn_id='my_postgres_connection')

        # SQL query to create table 
        create_table_query="""
                CREATE TABLE IF NOT EXISTS apod_data(
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(255),
                    explanation TEXT,
                    url TEXT,
                    date DATE,
                    media_type VARCHAR(50)  
                );
                        """
        # Execute table create query.
        postgres_hook.run(create_table_query)
        
    # Step-2 Extract NASA API data(APOD) Astronomy Picture of the Day
    #   https://api.nasa.gov/planetary/apod?api_key=yWQUOgVwCPGmK8GboYPX6GooZ1lQsSO79avg1Ahz

    extract_apod= HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', # Connection ID defined in Airflow for NASA API
        endpoint='planetary/apod',# NASA API endpoint for APOD
        method='GET',
        data={'api_key':'{{conn.nasa_api.extra_dejson.api_key}}'}, # API key 
        response_filter= lambda response:response.json()
    )


    # Transform the data
    @task 
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title',''),
            'explanation': response.get('explanation',''),
            'url': response.get('url',''),
            'date': response.get('date',''),
            'media_type': response.get('media_type','')
        }
        return apod_data

    # Step-3 Load the data into Postgres SQL
    @task 
    def load_data_to_postgres(apod_data):
        postgres_hook= PostgresHook(postgres_conn_id='my_postgres_connection')

        # Define SQL insert query
        insert_query="""
                INSERT INTO apod_data (title, explanation, url, date, media_type)
                VALUES(%s, %s, %s, %s, %s)
                """
        postgres_hook.run(insert_query,parameters=(
                apod_data['title'],
                apod_data['explanation'],
                apod_data['url'],
                apod_data['date'],
                apod_data['media_type']
        ))

    # Step-4 Define the dependencies
    ## Extract
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output
    ## Transform
    transformed_data=transform_apod_data(api_response)
    ## Load
    load_data_to_postgres(transformed_data)