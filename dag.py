from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Конфигурация
DB_CONN = 'gp_sapience_std12_50'
DB_SCHEMA = 'std12_50'

DICT_TABLES = {
    'chanel_ext': 'chanel',
    'price_ext': 'price',
    'product_ext': 'product',
    'region_ext': 'region'
}

FACT_TABLES = {
    'plan_ext': 'plan',
    'sales_ext': 'sales'
}

default_args = {
    'owner': 'std12_50',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=1)
}

with DAG(
        'gp_sapience_prod_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        max_active_runs=2,
        tags=['greenplum']
) as dag:
    start = DummyOperator(task_id='start')

    # Загрузка справочников
    with TaskGroup('dictionary_load') as dict_load:
        for src, dst in DICT_TABLES.items():
            PostgresOperator(
                task_id=f'load_{dst}',
                postgres_conn_id=DB_CONN,
                sql=f"""
                SELECT {DB_SCHEMA}.f_full_load(
                    '{DB_SCHEMA}.{src}',
                    '{dst}',
                    True
                )
                """
            )

    # Загрузка фактов
    with TaskGroup('fact_load') as fact_load:
        for src, dst in FACT_TABLES.items():
            PostgresOperator(
                task_id=f'load_{dst}',
                postgres_conn_id=DB_CONN,
                sql=f"""
                SELECT {DB_SCHEMA}.f_delta_partition(
                    '{DB_SCHEMA}',
                    '{src}',
                    '{dst}',
                    'date',
                    '2021-01-01',
                    '2021-12-31'
                )
                """
            )

    # Построение Data Mart
    build_mart = PostgresOperator(
        task_id='build_data_mart',
        postgres_conn_id=DB_CONN,
        sql=f"SELECT {DB_SCHEMA}.f_calculate_data_mart('2021', '07')"
    )

    end = DummyOperator(task_id='end')

    # Оркестрация
    start >> dict_load >> fact_load >> build_mart >> end