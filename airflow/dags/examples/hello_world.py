from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG('hello_world', schedule_interval='@daily', start_date=datetime(2024, 1, 1))
task = PythonOperator(task_id='hello', python_callable=lambda: print("Hello!"), dag=dag)
