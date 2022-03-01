import pendulum

from textwrap import dedent

from airflow.decorators import dag, task

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.oracle.hooks.oracle import OracleHook

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
def oeadwh_person_migrate_v2():
    @task()
    def extract_data():
        sql = dedent("""
        SELECT
            PERSON.OFFICER_CODE,
            PERSON.OFFICER_TYPE,
            PERSON.PREFIX_NAME_TH,
            PERSON.FIRST_NAME_TH,
            PERSON.MIDDLE_NAME_TH,
            PERSON.LAST_NAME_TH,
            PERSON.PREFIX_NAME_EN,
            PERSON.FIRST_NAME_EN,
            PERSON.MIDDLE_NAME_EN,
            PERSON.LAST_NAME_EN,
            PERSON.OFFICER_STATUS_CODE,
            PERSON.OFFICER_STATUS_DESC,
            PERSON.ACTIVE_FLAG,
            PERSON.IDCARD,
            CASE
                WHEN PERSON.CAMPUS_CODE = 'Z' THEN 'S'
                WHEN PERSON.CAMPUS_CODE = 'S' THEN 'C'
                ELSE PERSON.CAMPUS_CODE
            END CAMPUS_CODE,
            PERSON.CAMPUS_NAME_TH,
            PERSON.CAMPUS_NAME_EN,
            PERSON.FACULTY_CODE,
            PERSON.FACULTY_NAME_TH,
            PERSON.FACULTY_NAME_EN,
            PERSON.DEPARTMENT_CODE,
            PERSON.DEPARTMENT_NAME_TH,
            PERSON.DEPARTMENT_NAME_EN,
            PERSON.PROJECT_CODE,
            PERSON.PROJECT_NAME_TH,
            PERSON.PROJECT_NAME_EN,
            PERSON.NONTRI_ACCOUNT,
            ADV.ADVISOR_ID
        FROM KUEDU.V_LINK_HR_ALL PERSON
        LEFT JOIN KUEDU.V_LINK_ADVISOR_CURRENT ADV ON (ADV.OFFICER_CODE = PERSON.OFFICER_CODE)
        """)
        oracle_hook = OracleHook(oracle_conn_id='kuisea_readonly')
        oracle_conn = oracle_hook.get_conn()
        oracle_cursor = oracle_conn.cursor()
        oracle_cursor.execute(sql)
        return oracle_cursor.fetchall()
    @task()
    def load_data(data: list):
        postgres_hook = PostgresHook(postgres_conn_id='regdtb_dwh')
        postgres_hook.insert_rows('oeadwh.sync_person_m_personnel', data)
    t1 = PostgresOperator(
        task_id='create_sync_table',
        sql=dedent("""
        create table if not exists oeadwh.sync_person_m_personnel (
            officer_code character(8) not null primary key,
            officer_type int2,
            prefix_name_th character varying,
            first_name_th character varying,
            middle_name_th character varying,
            last_name_th character varying,
            prefix_name_en character varying,
            first_name_en character varying,
            middle_name_en character varying,
            last_name_en character varying,
            officer_status_code character varying,
            officer_status_desc character varying,
            active_flag character(1),
            idcard character varying,
            campus_code character varying,
            campus_name_th character varying,
            campus_name_en character varying,
            faculty_code character varying,
            faculty_name_th character varying,
            faculty_name_en character varying,
            department_code character varying,
            department_name_th character varying,
            department_name_en character varying,
            project_code character varying,
            project_name_th character varying,
            project_name_en character varying,
            nontri_account character varying,
            advisor_id character varying
        )
        """),
        postgres_conn_id='regdtb_dwh',
    )
    t2 = PostgresOperator(
        task_id='prepare_sync_table',
        sql='truncate table oeadwh.sync_person_m_personnel',
        postgres_conn_id='regdtb_dwh',
    )
    t3 = extract_data()
    t4 = load_data(t3)
    t5 = PostgresOperator(
        task_id='prepare_person_table',
        sql="UPDATE oeadwh.person_m_personnel SET data_updated_dt=CURRENT_TIMESTAMP, record_status='I'",
        postgres_conn_id='regdtb_dwh'
    )
    t6 = PostgresOperator(
        task_id='merge_data_from_sync_table',
        sql=dedent("""
        INSERT
            INTO
            oeadwh.person_m_personnel (
            officer_code,
            officer_type,
            prefix_name_th,
            first_name_th,
            middle_name_th,
            last_name_th,
            prefix_name_en,
            first_name_en,
            middle_name_en,
            last_name_en,
            officer_status_code,
            officer_status_desc,
            active_flag,
            idcard,
            campus_code,
            campus_name_th,
            campus_name_en,
            faculty_code,
            faculty_name_th,
            faculty_name_en,
            department_code,
            department_name_th,
            department_name_en,
            project_code,
            project_name_th,
            project_name_en,
            nontri_account,
            advisor_id,
            created_dt,
            updated_dt,
            data_updated_dt,
            record_status
        )
        SELECT
            officer_code,
            officer_type,
            prefix_name_th,
            first_name_th,
            middle_name_th,
            last_name_th,
            prefix_name_en,
            first_name_en,
            middle_name_en,
            last_name_en,
            officer_status_code,
            officer_status_desc,
            active_flag,
            idcard,
            campus_code,
            campus_name_th,
            campus_name_en,
            faculty_code,
            faculty_name_th,
            faculty_name_en,
            department_code,
            department_name_th,
            department_name_en,
            project_code,
            project_name_th,
            project_name_en,
            nontri_account,
            advisor_id,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            'A'
        FROM
            oeadwh.sync_person_m_personnel
        ON
            CONFLICT (officer_code) DO
        UPDATE
        SET
            officer_type = EXCLUDED.officer_type,
            prefix_name_th = EXCLUDED.prefix_name_th,
            first_name_th = EXCLUDED.first_name_th,
            middle_name_th = EXCLUDED.middle_name_th,
            last_name_th = EXCLUDED.last_name_th,
            prefix_name_en = EXCLUDED.prefix_name_en,
            first_name_en = EXCLUDED.first_name_en,
            middle_name_en = EXCLUDED.middle_name_en,
            last_name_en = EXCLUDED.last_name_en,
            officer_status_code = EXCLUDED.officer_status_code,
            officer_status_desc = EXCLUDED.officer_status_desc,
            active_flag = EXCLUDED.active_flag,
            idcard = EXCLUDED.idcard,
            campus_code = EXCLUDED.campus_code,
            campus_name_th = EXCLUDED.campus_name_th,
            campus_name_en = EXCLUDED.campus_name_en,
            faculty_code = EXCLUDED.faculty_code,
            faculty_name_th = EXCLUDED.faculty_name_th,
            faculty_name_en = EXCLUDED.faculty_name_en,
            department_code = EXCLUDED.department_code,
            department_name_th = EXCLUDED.department_name_th,
            department_name_en = EXCLUDED.department_name_en,
            project_code = EXCLUDED.project_code,
            project_name_th = EXCLUDED.project_name_th,
            project_name_en = EXCLUDED.project_name_en,
            nontri_account = EXCLUDED.nontri_account,
            advisor_id = EXCLUDED.advisor_id,
            updated_dt = CURRENT_TIMESTAMP,
            data_updated_dt = CURRENT_TIMESTAMP,
            record_status = 'A'
        """),
        postgres_conn_id='regdtb_dwh',
    )
    t4 >> t5 >> t6
    t1 >> t2 >> t3
dag = oeadwh_person_migrate_v2()
