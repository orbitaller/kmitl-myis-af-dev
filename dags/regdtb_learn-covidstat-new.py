import requests

import pendulum

from datetime import timedelta

from textwrap import dedent

from airflow.decorators import dag, task

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import null
@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': pendulum.duration(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'dag': dag,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 2, 28, tz="Asia/Bangkok"),
    catchup=False,
    tags=['regdtb','learn','COVID-19'],
)
def regdtb_learn_covidstat_new():
    """
    ### TaskFlow API
    """
    @task()
    def create_covid19_schema():
        """
        #### Preparation task
        """
        sql = dedent("""
        CREATE TABLE IF NOT EXISTS regdtb.learn_covid19_stat (
            id serial NOT NULL PRIMARY KEY,
            txn_date date NOT NULL,
            new_case integer NULL,
            total_case integer NULL,
            new_case_excludeabroad integer NULL,
            total_case_excludeabroad integer NULL,
            new_death integer NULL,
            total_death integer NULL,
            new_recovered integer NULL,
            total_recovered integer NULL,
            updated_dt timestamp with time zone NOT NULL,
            data_updated_dt timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """)
        postgres_hook = PostgresHook(postgres_conn_id='regdtb_dwh')
        postgres_hook.run(sql)
    @task()
    def extract_covid19_data(previous_task: None):
        """
        #### Extract task
        """
        url = 'https://covid19.ddc.moph.go.th/api/Cases/today-cases-all'
        response = requests.get(url)
        return response.json()
    @task()
    def load_covid19_data_into_schema(extracted_data: dict):
        """
        #### Load task
        """
        sql = dedent("""
        INSERT INTO regdtb.learn_covid19_stat (
            txn_date,
            new_case,
            total_case,
            new_case_excludeabroad,
            total_case_excludeabroad,
            new_death,
            total_death,
            new_recovered,
            total_recovered,
            updated_dt,
            data_updated_dt
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP
        );
        """)
        postgres_hook = PostgresHook(postgres_conn_id='regdtb_dwh')
        for covid19_data in extracted_data:
            postgres_hook.run(sql, parameters=(
                covid19_data['txn_date'],
                covid19_data['new_case'],
                covid19_data['total_case'],
                covid19_data['new_case_excludeabroad'],
                covid19_data['total_case_excludeabroad'],
                covid19_data['new_death'],
                covid19_data['total_death'],
                covid19_data['new_recovered'],
                covid19_data['total_recovered'],
                covid19_data['update_date']
            ))
    created_schema = create_covid19_schema()
    extracted_data = extract_covid19_data(created_schema)
    load_covid19_data_into_schema(extracted_data)
regdtb_learn_covidstat_new_dag = regdtb_learn_covidstat_new()
