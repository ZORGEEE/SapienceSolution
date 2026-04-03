from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

DB_CONN = 'gp_sapience_std12_50'
DB_SCHEMA = 'std12_50'
FULL_LOAD_FUNC = 'f_full_load'
DELTA_PART_FUNC = 'f_delta_partition'
DATA_MART_FUNC = 'f_calculate_data_mart'

DICT_TABLES = {
    '': '',
    '': '',
    '': '',
    '': ''
}
FACT_TABLES = {
    '': '',
    '': ''
}

full_load_sql = ''
delta_part_sql = ''
data_mart_sql = ''


def _hello():
    print("hello")


default_args = {
    'owner': 'std12_50',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'gp_sapience_homework_dag',
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['greenplum', 'std12_50']
)

task_start = DummyOperator(task_id='start', dag=dag)

python_task = PythonOperator(
    task_id='python',
    python_callable=_hello,
    dag=dag
)

task_end = DummyOperator(task_id='end', dag=dag)

(
        task_start >>
        python_task >>
        task_end
)
