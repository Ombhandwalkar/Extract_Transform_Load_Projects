from airflow.operators import dag, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime, duration
from include.datasets import DATASET_COCKTAIL
from include.tasks import _get_cocktail, _check_size, _validate_cocktail_fields
from include.extractor.callbacks import _handle_empty_size, _handle_failed_dag_run

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    default_args={
        'retries':2,
        'retry_delay': duration(seconds=2)
    },
    tags=['ecom'],
    catchup=False,
    on_failure_callback=_handle_failed_dag_run
)

def extractor():
    get_cocktail= PythonOperator(
        task_id='get_cocktail',
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL],
        retry_exponential_backoff=True,
        max_retry_delay=duration(minutes=15)
    )

    @task_group()
    def checks():
        check_size= PythonOperator(
            task_id='check_size',
            python_callable=_check_size,
            on_failure_callback=_handle_empty_size
        )
        validate_fields= PythonOperator(
            task_id='validate_fields',
            python_callable=_validate_cocktail_fields
        )
        check_size >> validate_fields
    
    get_cocktail >> checks()

my_extractor= extractor()

if __name__=="__main__":
    my_extractor.test()