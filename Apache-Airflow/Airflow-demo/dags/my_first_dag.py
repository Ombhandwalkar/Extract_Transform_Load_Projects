from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# Task 1
def hello():
    print('Hey !, How are you ?')

# Task 2
def myself():
    print('I am fine.')


default_arg={
    'owner':'Om',
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2025,6,22)
}

with DAG(
    dag_id='my_self_dag',
    default_args=default_arg,
    schedule_interval='@daily'
)as dag:
    # First task callout
    hell_task=PythonOperator(
        task_id='task1',
        python_callable=hello
    )
    # Second task callout
    myself_task=PythonOperator(
        task_id='task2',
        python_callable=myself
    )
    hell_task >> myself_task 