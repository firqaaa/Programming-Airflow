from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.sensors import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator

dag = DAG(
    'assignment_2',
    schedule_interval='@daily',
    start_date=datetime(2021, 12, 16),
    catchup=False
)

task1 = MySqlOperator(
    task_id='create_table',
    mysql_conn_id="mysql_conn",
    sql="CREATE TABLE IF NOT EXISTS students (id int, name varchar(50));",
    dag=dag
)

task2 = MySqlOperator(
    task_id='insert_data',
    mysql_conn_id="mysql_conn",
    sql="INSERT INTO students VALUES (1, 'John'), (2, 'Mark'), (3, 'Kelly'), (4, 'Smith');",
    dag=dag
)

task3 = SqlSensor(
    task_id='check_data_arrived',
    conn_id="mysql_conn",
    sql="SELECT COUNT(*) FROM students;",
    poke_interval=10,
    timeout=150,
    dag=dag
)

task4 = MySqlOperator(
    task_id='create_backup_table',
    mysql_conn_id="mysql_conn",
    sql="CREATE TABLE students_backup AS (SELECT * FROM students LIMIT 0); INSERT INTO students_backup SELECT * FROM students;",
    dag=dag
)

task1 >> task2 >> task3 >> task4