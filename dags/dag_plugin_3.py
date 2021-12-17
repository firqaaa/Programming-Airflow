from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks import MySQLToPostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import DataTransferOperator, FileCountSensor

dag = DAG(
    'plugins_dag',
    schedule_interval=timedelta(1),
    start_date=datetime(2021, 12, 17),
    catchup=False
)

def trigger_hook():
    MySQLToPostgresHook().copy_table('mysql_conn', 'postgres_conn')
    print("done")

task1 = PythonOperator(
    task_id = 'mysql_to_postgres',
    python_callable = trigger_hook,
    dag = dag
)