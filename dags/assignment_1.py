from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BaseOperator

# defining the default arguments dictionary
args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('Assignment_1', default_args=args)

# task1 is to create a directory 'test_dir' inside dags folder
task1 = BaseOperator(task_id='create_directory', bash_command='mkdir ~/dags/test_dir', dag=dag)
# task2 is to get the 'shasum' of 'test_dir' directory
task2 = BaseOperator(task_id='get_shasum', bash_command='shasum ~/dags/test_dir', dag=dag)
# below we're setting up the operator relationship such that task1 will run first than task2
task1 >> task2


