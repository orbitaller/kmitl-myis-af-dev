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
def oeadwh_person_migrate():
    @task()
    def read_person_data():
        sql = dedent("""
        SELECT
            OFFICER_CODE,
            OFFICER_TYPE,
            PREFIX_NAME_TH,
            FIRST_NAME_TH,
            MIDDLE_NAME_TH,
            LAST_NAME_TH,
            PREFIX_NAME_EN,
            FIRST_NAME_EN,
            MIDDLE_NAME_EN,
            LAST_NAME_EN,
            OFFICER_STATUS_CODE,
            OFFICER_STATUS_DESC,
            ACTIVE_FLAG,
            IDCARD,
            CAMPUS_CODE,
            CAMPUS_NAME_TH,
            CAMPUS_NAME_EN,
            FACULTY_CODE,
            FACULTY_NAME_TH,
            FACULTY_NAME_EN,
            DEPARTMENT_CODE,
            DEPARTMENT_NAME_TH,
            DEPARTMENT_NAME_EN,
            PROJECT_CODE,
            PROJECT_NAME_TH,
            PROJECT_NAME_EN,
            NONTRI_ACCOUNT
        FROM KUEDU.V_LINK_HR_ALL
        """)
        oracle_hook = OracleHook(oracle_conn_id='kuisea_readonly')
        oracle_conn = oracle_hook.get_conn()
        oracle_cursor = oracle_conn.cursor()
        oracle_cursor.execute(sql)
        return oracle_cursor.fetchall()
    @task()
    def load_person_data(person_data: dict):
        sql = dedent("""
        INSERT INTO oeadwh.person_m_personnel (
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
        VALUES (
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
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP,
            'A'
        ) ON CONFLICT ON CONSTRAINT person_m_personnel_pkey
        DO UPDATE SET
            officer_type = %s,
            prefix_name_th = %s,
            first_name_th = %s,
            middle_name_th = %s,
            last_name_th = %s,
            prefix_name_en = %s,
            first_name_en = %s,
            middle_name_en = %s,
            last_name_en = %s,
            officer_status_code = %s,
            officer_status_desc = %s,
            active_flag = %s,
            idcard = %s,
            campus_code = %s,
            campus_name_th = %s,
            campus_name_en = %s,
            faculty_code = %s,
            faculty_name_th = %s,
            faculty_name_en = %s,
            department_code = %s,
            department_name_th = %s,
            department_name_en = %s,
            project_code = %s,
            project_name_th = %s,
            project_name_en = %s,
            nontri_account = %s,
            updated_dt = CURRENT_TIMESTAMP,
            data_updated_dt = CURRENT_TIMESTAMP,
            record_status = 'A'
        """)
        postgres_hook = PostgresHook(postgres_conn_id='regdtb_dwh')
        for personnel_data in person_data:
            postgres_hook.run(sql, parameters=(
                personnel_data[0],
                personnel_data[1],
                personnel_data[2],
                personnel_data[3],
                personnel_data[4],
                personnel_data[5],
                personnel_data[6],
                personnel_data[7],
                personnel_data[8],
                personnel_data[9],
                personnel_data[10],
                personnel_data[11],
                personnel_data[12],
                personnel_data[13],
                personnel_data[14],
                personnel_data[15],
                personnel_data[16],
                personnel_data[17],
                personnel_data[18],
                personnel_data[19],
                personnel_data[20],
                personnel_data[21],
                personnel_data[22],
                personnel_data[23],
                personnel_data[24],
                personnel_data[25],
                personnel_data[26],
                None,
                personnel_data[1],
                personnel_data[2],
                personnel_data[3],
                personnel_data[4],
                personnel_data[5],
                personnel_data[6],
                personnel_data[7],
                personnel_data[8],
                personnel_data[9],
                personnel_data[10],
                personnel_data[11],
                personnel_data[12],
                personnel_data[13],
                personnel_data[14],
                personnel_data[15],
                personnel_data[16],
                personnel_data[17],
                personnel_data[18],
                personnel_data[19],
                personnel_data[20],
                personnel_data[21],
                personnel_data[22],
                personnel_data[23],
                personnel_data[24],
                personnel_data[25],
                personnel_data[26],
            ))
    t1 = PostgresOperator(
        task_id='prepare_person_table',
        sql="UPDATE oeadwh.person_m_personnel SET data_updated_dt=CURRENT_TIMESTAMP, record_status='I'",
        postgres_conn_id='regdtb_dwh'
    )
    t2 = read_person_data()
    t1 >> t2
    load_person_data(t2)
dag = oeadwh_person_migrate()
