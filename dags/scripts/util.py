from airflow.exceptions import AirflowException
import logging
from datetime import datetime, timezone
import subprocess
import os
import yaml
import glob

def run_cmd(cmd, return_output=False,
            exit_error="Something went wrong."):
    """
    Auxiliary function to run a cmd command.
    """
    logging.info(cmd)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    stdout, stderr = process.communicate()  # Waits for the command to finish. Otherwise, return code is always none.
    print(stdout.decode())
    if process.returncode != 0:
        raise AirflowException(exit_error)


def timestamp_to_millis(human_readable_timestamp):
    return round(
        datetime.strptime(human_readable_timestamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp() * 1000)


def parse_study_id(study_path, error_msg):
    meta_study = os.path.join(study_path, 'meta_study.txt')
    with open(meta_study, 'r') as f:
        for line in f:
            if 'cancer_study_identifier' in line:
                study_id = line.split(':')[1].strip().strip()
                return study_id
    raise AirflowException(error_msg)


def find_gene_panels(to_dir):
    meta_files = glob.glob(os.path.join(to_dir, 'meta_*'))
    # Filter out gene panel matrix file
    meta_content = [yaml.full_load(open(f)) for f in meta_files]
    panel_matrix_files = [meta.get('data_filename')
                          for meta in meta_content
                          if meta.get('genetic_alteration_type') == 'GENE_PANEL_MATRIX']
    meta_files_base = [os.path.basename(f) for f in meta_files]
    panel_files = [os.path.basename(f) for f in glob.glob(os.path.join(to_dir, '*gene_panel*'))]
    return list(filter(lambda f: f not in panel_matrix_files + meta_files_base, panel_files))


def get_timestamp(ti):
    from datetime import datetime
    current_timestamp_millis = int(ti.xcom_pull(key='timestamp', task_ids='push_timestamp'))
    return datetime.fromtimestamp((current_timestamp_millis / 1000.0)).strftime('%Y%m%d-%H%M%S')


def push_timestamp(**kwargs):
    """Pushes the current timestamp to XCOM."""
    import time
    ti = kwargs['ti']
    timestamp = round(time.time() * 1000)
    ti.xcom_push(key='timestamp', value=timestamp)