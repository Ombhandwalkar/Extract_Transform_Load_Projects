from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
from include.datasets import DATASET_COCKTAIL

@dag(
    start_date='2025-01-01',
    schedule='@daily',
    catchup=False,
    description='This DAG Process Ecommerce data',
    tags=['team_a','ecom'],  # Giving TAG helps to understand in team
    default_args={'retries':1}, # If particular task fails in their first attempt, then it will retry again for 1 time
    dagrun_timeout=duration(minutes=20), # Time limit for the dag run
    max_consecutive_failed_dag_runs=2    # If your DAG's consecutive 2 tasks failed then it will stop DAG.
)
def ecom():
    ta= EmptyOperator(task_id='ta')

ecom()