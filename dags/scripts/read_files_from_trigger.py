from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook
import os


def download_file_from_s3(**kwargs):
    ti = kwargs['task_instance']
    dag_vars = kwargs['params']  # dictionary of global variables from trigger
    datafiles_dir = kwargs['datafiles_dir']
    xcom_key_original_filename = kwargs['xcom_key_original_filename']
    connection_to_s3 = kwargs['connection_to_s3']

    # Read filename found by cbio_trigger from global parameters
    s3_file = dag_vars['op_kwargs']
    bucket = s3_file['bucket']
    key = s3_file['key']

    # Download the file
    hook = S3Hook(connection_to_s3)
    tmp_filename = hook.download_file(key=key, bucket_name=bucket, local_path=datafiles_dir)
    if not os.path.isfile(tmp_filename):
        raise AirflowException("Could not find downloaded file at path " + tmp_filename)
    # Rename from <datafiles_dir>/<tmp_filename> to <datafiles_dir>/<key>
    filename = os.path.join(datafiles_dir, key)
    os.rename(tmp_filename, filename)

    # Push <datafiles_dir>/<key> to XCOM
    ti.xcom_push(key=xcom_key_original_filename, value=key)


def read_file_from_local(**kwargs):
    ti = kwargs['task_instance']
    dag_vars = kwargs['params']  # dictionary of global variables from trigger
    datafiles_dir = kwargs['datafiles_dir']
    xcom_key_original_filename = kwargs['xcom_key_original_filename']

    # Read filename found by cbio_trigger from global parameters
    local_file = dag_vars['op_kwargs']
    key = local_file['key']

    # Push <datafiles_dir>/<key> to XCOM
    ti.xcom_push(key=xcom_key_original_filename, value=key)

def download_files_from_ftp(**kwargs):
    ti = kwargs['task_instance']
    dag_vars = kwargs['params']  # dictionary of global variables from trigger
    datafiles_source_dir = kwargs['datafiles_dir']
    xcom_key_original_filenames = kwargs['xcom_key_original_filename']

