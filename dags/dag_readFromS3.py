from datetime import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
#import pandas as pd
from scripts.ScannedFilesTracker import ScannedFilesTracker

S3_BUCKET_NAME = 'cds-cbioportal-test-data'
TRACKER_FILE = 'scanned_files_tracker.csv'

@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 7, 24, tz="UTC"),
    catchup=False,
    tags=["example"],
    default_args={
        'owner': 'thehyve',
        'retries': 1,
    },
)
def list_s3_contents_and_save():
    """
    DAG to list updated S3 bucket contents and save the list in a tracker file.
    """
    @task
    def list_s3_contents(bucket_name: str) -> list:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name)
        files = [obj['Key'] for obj in response.get('Contents', [])]
        return files

    @task
    def save_list_to_s3(bucket_name: str, tracker_file: str, files: list):
        # Initialize the tracker
        tracker = ScannedFilesTracker(tracker_file)
        
        # Filter out already scanned files
        new_files = [file for file in files if not tracker.has_file(file, datetime.now())]

        # Add new files to the tracker
        for file in new_files:
            tracker.add_scanned_file(file, datetime.now())

        # Save the tracker
        tracker.save_and_close({file: datetime.now() for file in new_files}, tracker_file)

        # Prepare the content to save to S3
        file_content = '\n'.join(new_files)
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).put_object(
            Key=tracker_file, 
            Body=file_content,
            ServerSideEncryption='aws:kms'
        )

    files = list_s3_contents(S3_BUCKET_NAME)
    save_list_to_s3(S3_BUCKET_NAME, TRACKER_FILE, files)

list_s3_contents_and_save_dag = list_s3_contents_and_save()