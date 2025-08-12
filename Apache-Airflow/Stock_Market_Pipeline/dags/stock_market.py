from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import io

# Stock Name
SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has succeded',
        channel='general'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has failed',
        channel='general'
    )

)
def stock_market():
    
    # Checking is API available or not 
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    # Fetching the stock data
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
    )
    
    # Storing the data into minIO storage server
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    
    # Performaing the transformation using Apache-spark and storing on minIO
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    
    # Getting formatted file 
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    

    # Storing our formatted file in Postgres(WH)
    def load_from_minio_to_postgres(**context):
        s3 = S3Hook(aws_conn_id='minio')
        postgres = PostgresHook(postgres_conn_id='postgres')
        
        # Pull the file from S3/MinIO
        bucket_name = BUCKET_NAME
        key = context['ti'].xcom_pull(task_ids='get_formatted_csv')
        file_obj = s3.get_key(key, bucket_name)
        
        # Read into pandas
        df = pd.read_csv(io.BytesIO(file_obj.get()['Body'].read()))
        
        # Load into Postgres
        engine = postgres.get_sqlalchemy_engine()
        df.to_sql('stock_market', engine, schema='public', if_exists='append', index=False)

    load_to_dw = PythonOperator(
        task_id='load_to_dw',
        python_callable=load_from_minio_to_postgres
       # provide_context=True
    )


    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw
        

stock_market()
    