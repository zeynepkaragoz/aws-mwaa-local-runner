from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

def list_packages():
    result = subprocess.run(['pip', 'freeze'], stdout=subprocess.PIPE)
    print(result.stdout.decode('utf-8'))

with DAG(
    dag_id='list_packages',
    start_date=datetime(2024, 8, 15),
    schedule_interval=None,
    catchup=False
) as dag:
    
    list_packages_task = PythonOperator(
        task_id='list_packages',
        python_callable=list_packages
    )
