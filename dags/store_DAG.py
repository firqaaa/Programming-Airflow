from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

from preproc import preproc

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('store_dag', default_args=default_args,
          schedule_interval='@daily',
          template_searchpath=['/usr/local/airflow/sql_files'], catchup=False)

task1 = BashOperator(task_id='check_file_exist',
                     bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
                     retries=2,
                     retry_delay=timedelta(seconds=15),
                     dag=dag)

task2 = PythonOperator(task_id='clean_raw_csv', python_callable=preproc, dag=dag)

task3 = MySqlOperator(task_id='create_mysql_table', mysql_conn_id='mysql_conn',
                      sql='create_table.sql')

task4 = MySqlOperator(task_id='insert_into_table', mysql_conn_id='mysql_conn',
                      sql='insert_into_table.sql', dag=dag)


task1 >> task2 >> task3 >> task4
