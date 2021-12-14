import airflow
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2021, 12, 13)
}

dag = DAG(
    "variable",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

t1 = BashOperator(
    task_id="print_path",
    bash_command="echo {{var.value.source_path}}",
    dag=dag
)