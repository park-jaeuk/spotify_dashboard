from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import pandas as pd
from typing import List, Tuple
import logging
import os
import pendulum
import glob
from utils.constant_util import *

# TODO : selenium 크롤링 구현하기
# def get_csv_files()

def upload_raw_files_to_s3(bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id="aws_s3")
    src_path = os.path.join(DOWNLOADS_DIR, f'spotify/charts/{NOW_DATE}/*.csv')

    filenames = glob.glob(src_path)
    logging.info(filenames[0])

    for filename in filenames:
        key = filename.replace(DOWNLOADS_DIR, '')
        key = os.path.join('raw_data', key[1:])
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

# TODO: charts S3에 담는거 추가하기

# # timezone 설정
# local_tz = pendulum.timezone("Asia/Seoul")
# # 현재 시간 설정
# NOW_DATE = datetime.now(tz=local_tz).strftime('%Y-%m-%d')
US_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
# US_DATE = "2024-03-22"
NOW_DATE = "2024-03-11"

with DAG(dag_id="spotify_charts_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )
    
    upload_raw_files_to_s3_task = PythonOperator(
        task_id = "upload_raw_files_to_s3_task",
        python_callable= upload_raw_files_to_s3,
        op_kwargs= {
            "bucket_name": BUCKET_NAME
        }
    )

    # spotify_api_dag trigger
    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='spotify_api_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    # start_task >> upload_raw_files_to_s3_task >> end_task

    # start_task >> transform_and_concat_csv_task >> call_trigger_task >> end_task

    start_task >> end_task