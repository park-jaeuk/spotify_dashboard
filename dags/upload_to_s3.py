from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from utils import constant_util

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook("aws_s3")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(dag_id="upload_to_s3",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start = EmptyOperator(
        task_id="start"
        )

    upload = PythonOperator(
        task_id = "upload_s3",
        python_callable= upload_to_s3,
        op_kwargs= {
            "filename": "/opt/airflow/data/spotify_album_api.json",
            "key": "data/spotify_album_api.json",
            "bucket_name": constant_util.BUCKET_NAME
        }
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> upload >> end