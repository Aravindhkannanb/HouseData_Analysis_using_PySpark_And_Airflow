from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'my_first_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 10, 14),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=my_task,
    )
