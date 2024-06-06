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
from utils.constant_util import *
from utils import common_util


def transform_track_csv() -> None:
    columns = ['spotify_track_id', 'spotify_album_id', 'name', 'duration_ms']
    track_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(DOWNLOADS_DIR, f'spotify/api/tracks')

    for track_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(track_json_path, "r") as track_json:
            track_api = json.load(track_json)

        track_dict['spotify_track_id'].append(track_api['id'])
        track_dict['spotify_album_id'].append(track_api['album']['id'])
        track_dict['name'].append(track_api['name'])
        track_dict['duration_ms'].append(track_api['duration_ms'])
    
    track_df = pd.DataFrame(track_dict, columns = columns)
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/tracks/{US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_track.csv")
    track_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_album_csv() -> None:
    columns = ['spotify_album_id', 'name', 'total_tracks', 'album_type', 'release_date', 'release_date_precision']
    album_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(DOWNLOADS_DIR, f'spotify/api/albums')

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
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/albums/{US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_album.csv")
    album_df.to_csv(dst_file_path, encoding='utf-8-sig', index=False)

def transform_artist_csv() -> None:
    columns = ['spotify_artist_id', 'name', 'type']
    artist_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(DOWNLOADS_DIR, f'spotify/api/artists')

    for album_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(album_json_path, "r") as artist_json:
            artist_api = json.load(artist_json)
        
        artist_dict['spotify_artist_id'].append(artist_api['id'])
        artist_dict['name'].append(artist_api['name'])
        artist_dict['type'].append(artist_api['type'])
    
    artist_df = pd.DataFrame(artist_dict, columns = columns)
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/artists/{US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_artist.csv")
    artist_df.to_csv(dst_file_path, encoding='utf-8-sig', index=False)

def transform_track_artist_csv() -> None:
    columns = ['spotify_track_id', 'spotify_artist_id']
    track_artist_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(DOWNLOADS_DIR, f'spotify/api/tracks')

    for track_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(track_json_path, "r") as track_json:
            track_api = json.load(track_json)

        for artist_info in track_api['artists']:
            track_artist_dict['spotify_track_id'].append(track_api['id'])
            track_artist_dict['spotify_artist_id'].append(artist_info['id'])
    
    track_artist_df = pd.DataFrame(track_artist_dict, columns = columns)
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/track-artists/{US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"transform_track_artist.csv")
    track_artist_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_track_chart_csv() -> None:
    # 트랙 테이블에서 spotify_id(외부 아이디)로 id 값을 가져와서 track_info 연결하기
    src_dir_path = os.path.join(DOWNLOADS_DIR, f'spotify/charts/{US_DATE}')
    src_files = os.path.join(src_dir_path, "*.csv")
    
    columns = ['spotify_track_id', 'now_rank', 'peak_rank', 'previous_rank', 
                'total_days_on_chart', 'stream_count', 'region', 'chart_date']

    concat_df = pd.DataFrame(columns = columns)

    filenames = glob.glob(src_files)
    for filename in filenames:
        df = pd.read_csv(filename)
        
        # 데이터 변환 
        df['spotify_track_id'] = df['uri'].str.split(':').str[-1]
        df['chart_date'] = US_DATE
        df['region'] = filename.split('/')[-1].split('-')[1] 

        # 컬럼 이름 변경
        rename_columns = {'rank' : 'now_rank', 'streams':'stream_count', 'days_on_chart':'total_days_on_chart'}
        df.rename(columns = rename_columns, inplace = True)

        # 필요없는 삭제 및 컬럼 순서 변경
        df = df[columns]

        logging.info(len(df))

        concat_df = pd.concat([concat_df, df])

    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/track-charts/{US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True) 


    dst_file = os.path.join(dst_dir_path, f'transform_track_chart.csv')
    concat_df.to_csv(dst_file, index=False)

    logging.info(dst_file)
    logging.info(len(concat_df))


def upload_transform_album_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR, f'spotify/albums/{US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_album.csv"))
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

def upload_transform_track_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR, f'spotify/tracks/{US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_track.csv"))
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

def upload_transform_artist_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR, f'spotify/artists/{US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_artist.csv"))
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

def upload_transform_track_artist_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR, f'spotify/track-artists/{US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_track_artist.csv"))
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

def upload_transform_track_chart_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR, f'spotify/track-charts/{US_DATE}')
    filenames = glob.glob(os.path.join(src_path, f"transform_track_chart.csv"))
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True) 

###########################################################################################################################
## last fm

def transform_reviews_csv() -> None:
    review_dict = {}
    src_dir_path = os.path.join(DOWNLOADS_DIR, f'last_fm/reviews')

    for review_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(review_json_path, "r", encoding='UTF-8') as review_json:
            review_api = json.load(review_json)
            reviews = review_api[review_json_path.replace('\\','/').split('/')[-1].replace('.json','')]['reviews']
            if len(reviews) == 0:
                continue
        review_dict = {}
        columns = ['spotify_track_id', 'review', 'date', 'likes']
        review_dict = {column : [] for column in columns}
        spotify_track_id = list(review_api.keys())[0]
        for review in reviews:
            review_dict['spotify_track_id'].append(spotify_track_id)
            review_dict['review'].append(review['review'])
            review_dict['date'].append(review['date'])
            review_dict['likes'].append(review['likes'])
        review_df = pd.DataFrame(review_dict, columns = columns).drop_duplicates()
        dst_dir_path = os.path.join(TRANSFORM_DIR, 'last_fm/reviews')
        os.makedirs(dst_dir_path, exist_ok=True)
        dst_file_path = os.path.join(dst_dir_path, f"{spotify_track_id}.csv")
        review_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_information_csv() -> None:
    columns = ['spotify_track_id','listeners', 'length', 'introduction']
    info_dic = {column: [] for column in columns}

    src_dir_path = os.path.join(DOWNLOADS_DIR, f'last_fm/information')
    transform_df = pd.DataFrame(columns = columns)
    for info_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(info_json_path, "r", encoding='UTF-8') as info_json:
            info_api = json.load(info_json)
        spotify_track_id = list(info_api.keys())[0]
        info_dic = info_api[spotify_track_id]
        info_dic['spotify_track_id'] = spotify_track_id
        info_dic.pop('genres')
        info_df = pd.DataFrame([info_dic])
        transform_df = pd.concat([transform_df,info_df])

    transform_df = transform_df.drop_duplicates()
    transform_df.columns = ['spotify_track_id','listeners', 'duration', 'introduction', 'last_fm_url']
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'last_fm/information')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, "total_information.csv")
    transform_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_tags_csv() -> None:
    columns = ['spotify_track_id', 'tags']
    info_dict = {column: [] for column in columns}

    src_dir_path = os.path.join(DOWNLOADS_DIR, f'last_fm/information')
    transform_df = pd.DataFrame(columns = columns)
    for info_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(info_json_path, "r", encoding='UTF-8') as info_json:
            info_api = json.load(info_json)
        spotify_track_id = list(info_api.keys())[0]
        tags = info_api[spotify_track_id]['genres']
        spotify_track_id_lst = [spotify_track_id] * len(tags)

        info_dict['spotify_track_id'] = spotify_track_id_lst
        info_dict['tags'] = tags
        info_df = pd.DataFrame(info_dict, columns = columns)
        transform_df = pd.concat([transform_df,info_df])

    transform_df = transform_df.drop_duplicates()    
    dst_dir_path = os.path.join(TRANSFORM_DIR, f'last_fm/tags')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"total_tags.csv")
    transform_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def upload_transform_reviews_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR,'last_fm/reviews/*.csv')
    filenames = glob.glob(src_path)
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]
    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

def upload_transform_information_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR,'last_fm/information/*.csv')
    filename = glob.glob(src_path)[0]
    key = filename.replace(AIRFLOW_HOME, "")[1:]
    common_util.upload_file_to_s3(filename=filename, key=key, bucket_name=bucket_name, replace=True)

def upload_transform_tags_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(TRANSFORM_DIR,'last_fm/tags/*.csv')
    filename = glob.glob(src_path)[0]
    key = filename.replace(AIRFLOW_HOME, "")[1:]
    common_util.upload_file_to_s3(filename=filename, key=key, bucket_name=bucket_name, replace=True)

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
            "bucket_name": BUCKET_NAME
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
            "bucket_name": BUCKET_NAME
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
            "bucket_name": BUCKET_NAME
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
            "bucket_name": BUCKET_NAME
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
            "bucket_name": BUCKET_NAME
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

