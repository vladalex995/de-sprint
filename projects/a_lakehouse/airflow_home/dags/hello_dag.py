from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2025, 8, 1),
    schedule=None,          # no schedule; you trigger it manually
    catchup=False,
    tags=["test"],
):
    BashOperator(task_id="say_hi", bash_command="echo hello airflow")
