from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator

from preproc import preproc

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 12, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('store_dag', default_args=default_args,
          schedule_interval='@daily',
          template_searchpath=['/usr/local/airflow/sql_files'], catchup=False)

# task1 = BashOperator(task_id='check_file_exist', bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
#                      retries=2,
#                      retry_delay=timedelta(seconds=15),
#                      dag=dag)

task1 = FileSensor(
    task_id='check_file_exists',
    filepath='/usr/local/airflow/store_files_airflow/raw_store_transactions.csv',
    fs_conn_id='fs_default',
    poke_interval=10,
    timeout=150,
    soft_fail=True
)

task2 = PythonOperator(task_id="clean_raw_csv", python_callable=preproc, dag=dag)

task3 = MySqlOperator(task_id="create_mysql_table", mysql_conn_id="mysql_conn", sql="create_table.sql", dag=dag)

task4 = MySqlOperator(task_id='insert_into_table', mysql_conn_id='mysql_conn',
                      sql='insert_into_table.sql', dag=dag)

task5 = MySqlOperator(task_id='select_from_table', mysql_conn_id='mysql_conn',
                      sql='select_from_table.sql', dag=dag)

task6 = BashOperator(task_id='move_file1', bash_command='cat ~/store_files_airflow/loc4_wise_profit.csv && mv ~/store_files_airflow/loc4_wise_profit.csv ~/store_files_airflow/loc4_wise_profit_%s.csv' % yesterday_date, dag=dag)

task7 = BashOperator(task_id='move_file2', bash_command='cat ~/store_files_airflow/str4_wise_profit.csv && mv ~/store_files_airflow/str4_wise_profit.csv ~/store_files_airflow/str4_wise_profit_%s.csv' % yesterday_date, dag=dag)

task8 = EmailOperator(task_id='send_email',
                      to='dataxalgo@gmail.com',
                      subject='Daily Report Generated',
                      html_content="""<h2> Congratulations! Your store reports are ready.</h2>""",
                      files=['/usr/local/airflow/store_files_airflow/loc4_wise_profit_%s.csv' % yesterday_date,
                             '/usr/local/airflow/store_files_airflow/str4_wise_profit_%s.csv' % yesterday_date],
                      dag=dag)

# task9 = BashOperator(task_id='rename_raw', bash_command='mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv' % yesterday_date, dag=dag)


task1 >> task2 >> task3 >> task4 >> task5 >> [task6, task7] >> task8
