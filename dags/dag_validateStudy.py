from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}


with DAG(
    dag_id='cbioportal_importer_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:


    run_importer_script = BashOperator(
        task_id='run_cbioportal_importer',
        bash_command='PORTAL_HOME=/usr/local/airflow/cbioportal-core python3 /usr/local/airflow/cbioportal-core/scripts/importer/metaImport.py --study_directory /usr/local/airflow/study_es_0_mini', 
    )

    run_importer_script

    