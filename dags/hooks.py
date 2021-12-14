from airflow import DAG
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date': datetime(2021, 12, 13),
}

dag = DAG(
    'hooks_demo',
    default_args=default_args,
    schedule_interval='@daily'
)

def transfer_function(ds, **kwargs):

    query = "SELECT * FROM source_city_table"

    #source hook
    source_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    source_conn = source_hook.get_conn()

    #destination hook
    destination_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    destination_conn = destination_hook.get_conn()

    source_cursor = source_conn.cursor()
    destination_cursor = destination_conn.cursor()

    source_cursor.execute(query)
    records = source_cursor.fetchall()

    if records:
        execute_values(destination_cursor, "INSERT INTO target_city_table VALUES %s", records)
        destination_conn.commit()

    source_cursor.close()
    destination_cursor.close()
    source_conn.close()
    destination_conn.close()
    print("Data transfered successfully!")

t1 = PythonOperator(task_id='transfer', python_callable=transfer_function, provide_context=True, dag=dag)
