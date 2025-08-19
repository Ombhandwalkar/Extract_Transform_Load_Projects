from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime
from cosmos.airflow.task_group import DbtTaskGroup 
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from airflow.models.baseoperator import chain
import datetime as dt

AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW="aa80d84a-20b7-4573-9a15-b2e1abc4068b"
AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW="14023cc3-6284-4954-97b1-fd682c40dfa8"
AIRBYTE_JOB_RAW_TO_STAGING="8c547ce9-adcf-459c-8776-1c903088cfbf"

@dag(
    start_date=dt.datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['airbyte','risk']
)
def customer_metrics():
    load_customer_transaction_raw= AirbyteTriggerSyncOperator(
        task_id='load_customer_transaction_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW

    )
    load_labeled_transaction_raw= AirbyteTriggerSyncOperator(
        task_id='load_labeled_transaction_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW

    )
    write_to_staging= AirbyteTriggerSyncOperator(
        task_id='write_to_staging',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW

    )

    @task
    def airbyte_job_done():
        return True
    

    @task.external_python(python='/opt/airflow/soda_venv/bin/python')
    def audit_customer_transaction(scan_name='customer_transactions',
                                    checks_subpath='tables',
                                    data_source='staging'):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task.external_python(python='/opt/airflow/soda_venv/bin/python')
    def audit_labeled_transaction(scan_name='labeled_transactions',
                                    checks_subpath='tables',
                                    data_source='staging'):
        from include.soda.helpers import check
        check(scan_name, checks_subpath, data_source)

    @task
    def quality_checks_done():
        return True
    
    publish= DbtTaskGroup(
        group_id='publish',
        project_config=DBT_PROJECT_CONFIG,
        profile_config= DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models']
        )
    )

    chain(
        [load_customer_transaction_raw, load_labeled_transaction_raw],
        write_to_staging,
        airbyte_job_done(),
        [audit_customer_transaction(), audit_labeled_transaction()],
        quality_checks_done(),
        publish
    )

customer_metrics()