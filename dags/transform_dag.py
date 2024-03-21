from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import pendulum
import glob
import json

SPOTIFY_CLIENT_ID = Variable.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET")
BUCKET_NAME = Variable.get("BUCKET_NAME")
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
RAW_DATA_DIR = os.path.join(AIRFLOW_HOME, 'raw_data')
TRANSFORM_DIR = os.path.join(AIRFLOW_HOME, 'transform')


# # timezone 설정
# local_tz = pendulum.timezone("Asia/Seoul")
# # 현재 시간 설정
# NOW_DATE = datetime.now(tz=local_tz).strftime('%Y-%m-%d')
NOW_DATE = "2024-03-11"

def transform_album_df() -> None:
    columns = ['spotify_id', 'name', 'total_tracks', 'album_type', 'release_date', 'release_date_precision']
    album_df = pd.DataFrame(columns = columns)
    
    album_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(RAW_DATA_DIR, f'spotify/api/albums')

    for album_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(album_json_path, "r") as album_json:
            album_api = json.load(album_json)
        
        album_dict['spotify_id'].append(album_api['id'])
        album_dict['name'].append(album_api['name'])
        album_dict['total_tracks'].append(album_api['total_tracks'])
        album_dict['album_type'].append(album_api['album_type'])
        album_dict['release_date'].append(album_api['release_date'])
        album_dict['release_date_precision'].append(album_api['release_date_precision'])
    
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/api/{NOW_DATE}/albums')
    dst_file_path = os.path.join(dst_dir_path, f"transfrom_album.csv")
    album_df.to_csv(dst_file_path, index=False)



with DAG(dag_id="transform_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    transform_album_df_task = PythonOperator(
        task_id = "transform_csv_task",
        python_callable=transform_album_df,
    )

    
    end_task = EmptyOperator(
        task_id="end_task"
    )

    start_task >> transform_album_df_task >> end_task