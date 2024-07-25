from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import glob
import json

from sql import url
from utils import common_util
from utils.constant_util import Directory, Config, Date


def transform_track_csv() -> None:
    columns = ['spotify_track_id', 'spotify_album_id', 'name', 'duration_ms']
    track_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'spotify/api/tracks')

    for track_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(track_json_path, "r") as track_json:
            track_api = json.load(track_json)

        track_dict['spotify_track_id'].append(track_api['id'])
        track_dict['spotify_album_id'].append(track_api['album']['id'])
        track_dict['name'].append(track_api['name'])
        track_dict['duration_ms'].append(track_api['duration_ms'])
    
    track_df = pd.DataFrame(track_dict, columns = columns)
    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/tracks/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_track.csv")
    track_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_album_csv() -> None:
    columns = ['spotify_album_id', 'name', 'total_tracks', 'album_type', 'release_date', 'release_date_precision']
    album_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'spotify/api/albums')

    for album_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(album_json_path, "r") as album_json:
            album_api = json.load(album_json)
        
        album_dict['spotify_album_id'].append(album_api['id'])
        album_dict['name'].append(album_api['name'])
        album_dict['total_tracks'].append(album_api['total_tracks'])
        album_dict['album_type'].append(album_api['album_type'])
        album_dict['release_date'].append(album_api['release_date'])
        album_dict['release_date_precision'].append(album_api['release_date_precision'])
    
    album_df = pd.DataFrame(album_dict, columns = columns)
    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/albums/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_album.csv")
    album_df.to_csv(dst_file_path, encoding='utf-8-sig', index=False)

def transform_artist_csv() -> None:
    columns = ['spotify_artist_id', 'name', 'type']
    artist_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'spotify/api/artists')

    for album_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(album_json_path, "r") as artist_json:
            artist_api = json.load(artist_json)
        
        artist_dict['spotify_artist_id'].append(artist_api['id'])
        artist_dict['name'].append(artist_api['name'])
        artist_dict['type'].append(artist_api['type'])
    
    artist_df = pd.DataFrame(artist_dict, columns = columns)
    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/artists/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_artist.csv")
    artist_df.to_csv(dst_file_path, encoding='utf-8-sig', index=False)

def transform_track_artist_csv() -> None:
    columns = ['spotify_track_id', 'spotify_artist_id']
    track_artist_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'spotify/api/tracks')

    for track_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(track_json_path, "r") as track_json:
            track_api = json.load(track_json)

        for artist_info in track_api['artists']:
            track_artist_dict['spotify_track_id'].append(track_api['id'])
            track_artist_dict['spotify_artist_id'].append(artist_info['id'])
    
    track_artist_df = pd.DataFrame(track_artist_dict, columns = columns)
    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/track-artists/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_track_artist.csv")
    track_artist_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_track_chart_csv() -> None:
    # 트랙 테이블에서 spotify_id(외부 아이디)로 id 값을 가져와서 track_info 연결하기
    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'spotify/charts/{Date.US_DATE}')
    src_files = os.path.join(src_dir_path, "*.csv")
    
    columns = ['spotify_track_id', 'now_rank', 'peak_rank', 'previous_rank', 
                'total_days_on_chart', 'stream_count', 'region', 'chart_date']

    concat_df = pd.DataFrame(columns = columns)

    filenames = glob.glob(src_files)
    for filename in filenames:
        df = pd.read_csv(filename)
        
        # 데이터 변환 
        df['spotify_track_id'] = df['uri'].str.split(':').str[-1]
        df['chart_date'] = Date.US_DATE
        df['region'] = filename.split('/')[-1].split('-')[1] 

        # 컬럼 이름 변경
        rename_columns = {'rank' : 'now_rank', 'streams':'stream_count', 'days_on_chart':'total_days_on_chart'}
        df.rename(columns = rename_columns, inplace = True)

        # 필요없는 삭제 및 컬럼 순서 변경
        df = df[columns]

        logging.info(len(df))

        concat_df = pd.concat([concat_df, df])

    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/track-charts/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True) 


    dst_file = os.path.join(dst_dir_path, f'transform_track_chart.csv')
    concat_df.to_csv(dst_file, index=False)

    logging.info(dst_file)
    logging.info(len(concat_df))


def upload_transform_album_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/albums/{Date.US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_album.csv"))
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

def upload_transform_track_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/tracks/{Date.US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_track.csv"))
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

def upload_transform_artist_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/artists/{Date.US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_artist.csv"))
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

def upload_transform_track_artist_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/track-artists/{Date.US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_track_artist.csv"))
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

def upload_transform_track_chart_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'spotify/track-charts/{Date.US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_track_chart.csv"))
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 


with DAG(dag_id="transform_spotify_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    transform_album_csv_task = PythonOperator(
        task_id = "transform_album_csv_task",
        python_callable=transform_album_csv,
    )

    upload_transform_album_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_album_csv_to_s3_task",
        python_callable=upload_transform_album_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    transform_artist_csv_task = PythonOperator(
        task_id = "transform_artist_csv_task",
        python_callable=transform_artist_csv
    )

    upload_transform_artist_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_artist_csv_to_s3_task",
        python_callable=upload_transform_artist_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    transform_track_csv_task = PythonOperator(
        task_id = "transform_track_csv_task",
        python_callable=transform_track_csv
    )

    upload_transform_track_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_track_csv_to_s3_task",
        python_callable=upload_transform_track_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    transform_track_chart_csv_task = PythonOperator(
        task_id = "transform_track_chart_csv_task",
        python_callable=transform_track_chart_csv
    )

    upload_transform_track_chart_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_track_chart_csv_to_s3_task",
        python_callable=upload_transform_track_chart_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    transform_track_artist_csv_task = PythonOperator(
        task_id = "transform_track_artist_csv_task",
        python_callable=transform_track_artist_csv
    )

    upload_transform_track_artist_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_track_artist_csv_to_s3_task",
        python_callable=upload_transform_track_artist_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )


    trigger_upload_to_snowflake_task = TriggerDagRunOperator(
        task_id='trigger_upload_to_snowflake_task',
        trigger_dag_id='upload_spotify_to_snowflake_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )


    end_task = EmptyOperator(
        task_id="end_task"
    )

    start_task >> [transform_album_csv_task,  transform_artist_csv_task
                   , transform_track_csv_task, transform_track_chart_csv_task, transform_track_artist_csv_task]
    
    transform_album_csv_task >> upload_transform_album_csv_to_s3_task
    transform_artist_csv_task >> upload_transform_artist_csv_to_s3_task
    transform_track_csv_task >> upload_transform_track_csv_to_s3_task
    transform_track_chart_csv_task >> upload_transform_track_chart_csv_to_s3_task
    transform_track_artist_csv_task >> upload_transform_track_artist_csv_to_s3_task

    [upload_transform_album_csv_to_s3_task, upload_transform_artist_csv_to_s3_task, 
     upload_transform_track_csv_to_s3_task, upload_transform_track_chart_csv_to_s3_task, upload_transform_track_artist_csv_to_s3_task] >> trigger_upload_to_snowflake_task

    
    trigger_upload_to_snowflake_task >> end_task

