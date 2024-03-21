from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models.variable import Variable

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import pendulum
import glob
import json

from sql import album_sql
from sql import artist_sql

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook("aws_s3")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

def transform_album_df() -> None:
    columns = ['spotify_id', 'name', 'total_tracks', 'album_type', 'release_date', 'release_date_precision']
    
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

    album_df = pd.DataFrame(album_dict)

    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/api/albums/{NOW_DATE}')

    os.makedirs(dst_dir_path, exist_ok=True) 

    dst_file_path = os.path.join(dst_dir_path, f"transform_album.csv")
    album_df.to_csv(dst_file_path, index=False)

def transform_artist_df() -> None:
    columns = ['spotify_id', 'name', 'type']
    
    artist_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(RAW_DATA_DIR, f'spotify/api/artists')

    for artist_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(artist_json_path, "r") as artist_json:
            artist_api = json.load(artist_json)
        
        artist_dict['spotify_id'].append(artist_api['id'])
        artist_dict['name'].append(artist_api['name'])
        artist_dict['type'].append(artist_api['type'])

    artist_df = pd.DataFrame(artist_dict)

    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/api/artists/{NOW_DATE}')

    os.makedirs(dst_dir_path, exist_ok=True) 

    dst_file_path = os.path.join(dst_dir_path, f"transform_artist.csv")
    artist_df.to_csv(dst_file_path, index=False)

def transform_track_chart_table() -> None:
    transform_columns = ['now_rank', 'peak_rank', 'previous_rank', 'total_days_on_chart',
                'stream_count', 'region', 'chart_date']
    
    csv_path = os.path.join(TRANSFORM_DIR, f"spotify/charts/{NOW_DATE}/regional-transform-daily-2024-03-11.csv")
    df = pd.read_csv(csv_path)
    df = df[transform_columns]

    chart_table_path = os.path.join(TRANSFORM_DIR, f"spotify/charts/{NOW_DATE}/track-chart-table-{NOW_DATE}.csv")
    df.to_csv(chart_table_path, index=False)


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
chart_table_path = os.path.join(TRANSFORM_DIR, f"spotify/charts/{NOW_DATE}/track-chart-table-{NOW_DATE}.csv")


with DAG(dag_id="upload_to_snowflake_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )
    
    transform_album_df_task = PythonOperator(
        task_id = "transform_album_df_task",
        python_callable=transform_album_df,
    )

    filename = os.path.join(TRANSFORM_DIR, f"spotify/api/albums/{NOW_DATE}/transform_album.csv")
    key = os.path.join("transform", f"spotify/api/albums/{NOW_DATE}/transform_album.csv")
    
    upload_transform_album_to_s3_task = PythonOperator(
        task_id = "upload_transform_album_to_s3_task",
        python_callable= upload_to_s3,
        op_kwargs= {
            "filename": filename,
            "key": key,
            "bucket_name": "airflow-gin-bucket"
        }
    )

    upload_transform_album_to_snowflake_task = SnowflakeOperator(
        task_id='upload_transform_album_to_snowflake_task',
        sql=album_sql.sql,
        snowflake_conn_id='snowflake_default',
    )

        
    transform_artist_df_task = PythonOperator(
        task_id = "transform_artist_df_task",
        python_callable=transform_artist_df,
    )

    filename = os.path.join(TRANSFORM_DIR, f"spotify/api/artists/{NOW_DATE}/transform_artist.csv")
    key = os.path.join("transform", f"spotify/api/artists/{NOW_DATE}/transform_artist.csv")

    upload_transform_artist_to_s3_task = PythonOperator(
        task_id = "upload_transform_artist_to_s3_task",
        python_callable= upload_to_s3,
        op_kwargs= {
            "filename": filename,
            "key": key,
            "bucket_name": "airflow-gin-bucket"
        }
    )

    upload_transform_artist_to_snowflake_task = SnowflakeOperator(
        task_id='upload_transform_artist_to_snowflake_task',
        sql=artist_sql.sql,
        snowflake_conn_id='snowflake_default',
    )

    # transform_track_chart_table_task = PythonOperator(
    #     task_id="transform_track_chart_table_task",
    #     python_callable=transform_track_chart_table
    # )

    # copy_into_task = SnowflakeOperator(
    #     task_id='copy_into_task',
    #     sql=sql,
    #     snowflake_conn_id='snowflake_default',
    # )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    
    # start_task >> transform_track_chart_table_task >> copy_into_task >> end_task
    start_task >> transform_album_df_task >> upload_transform_album_to_s3_task
    upload_transform_album_to_s3_task >> upload_transform_album_to_snowflake_task               
    upload_transform_album_to_snowflake_task >> end_task

    start_task >> transform_artist_df_task >> upload_transform_artist_to_s3_task
    upload_transform_artist_to_s3_task >> upload_transform_artist_to_snowflake_task               
    upload_transform_artist_to_snowflake_task >> end_task
    