from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import DataTransferOperator, FileCountSensor

dag = DAG(
    'plugin_dag_2',
    schedule_interval=timedelta(1),
    start_date=datetime(2021, 12, 16),
    catchup=False,
)

task1 = FileCountSensor(
    task_id = 'file_count_sensor',
    dir_path = '/usr/local/airflow/plugins',
    conn_id = 'fs_default',
    poke_interval = 5,
    timeout = 100,
    dag = dag
)