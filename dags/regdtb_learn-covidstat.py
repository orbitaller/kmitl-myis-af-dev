import json
import requests
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Utility Classes
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
}

with DAG(
    'regdtb_learn-covidstat',
    default_args=default_args,
    description='[regdtb] My test workflow to understand covid result',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 10, 25, 1, 30, 0),
    catchup=False,
    tags=['regdtb','learn','COVID-19'],
) as dag:
    def extract(**kwargs):
        ti = kwargs['ti']
        url = 'https://covid19.ddc.moph.go.th/api/Cases/today-cases-all'
        response = requests.get(url)
        data = response.text
        ti.xcom_push('covid19_today_data', data)
        return response.json()
    def load(**kwargs):
        sql = """
        INSERT INTO regdtb.learn_covid19_stat (txn_date, new_case, total_case, new_case_excludeabroad, total_case_excludeabroad, new_death, total_death, new_recovered, total_recovered, updated_dt, data_updated_dt)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
        """
        ti = kwargs['ti']
        extract_data_string = ti.xcom_pull(task_ids='extract_covid19_data', key='covid19_today_data')
        covid19_data = json.loads(extract_data_string)[0]
        postgres_hook = PostgresHook(postgres_conn_id='regdtb_dwh')
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
    t1 = PythonOperator(
        task_id='extract_covid19_data',
        python_callable=extract
    )
    t2 = PostgresOperator(
        task_id='create_covid19_schema',
        sql=dedent("""
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
        """),
        postgres_conn_id='regdtb_dwh'
    )
    t3 = PythonOperator(
        task_id='load_covid19_data_into_schema',
        python_callable=load
    )
    [t1, t2] >> t3
