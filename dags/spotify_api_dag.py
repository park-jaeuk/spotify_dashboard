from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import base64
import requests
import pendulum
import glob
import json
import math
from utils.constant_util import *
from utils import common_util

def get_access_token(spotify_client_id: str, spotify_client_secret: str) -> str:
    endpoint = 'https://accounts.spotify.com/api/token'

    encoded = base64.b64encode(f'{spotify_client_id}:{spotify_client_secret}'.encode('utf-8')).decode('ascii')

    headers = {'Authorization': f'Basic {encoded}',
               'Content-Type': 'application/x-www-form-urlencoded'
              }
    payload = {'grant_type': 'client_credentials'}

    response = requests.post(endpoint, data=payload, headers=headers)
    access_token = response.json()['access_token']
    
    return access_token

# snowflake에 있는지 확인할 track들에 list 모음 (merge into 실행하기)
def get_spotify_track_api(src_path: str, idx:int , **context) -> None:
    csv_path = os.path.join(TRANSFORM_DIR, src_path)
    df = pd.read_csv(csv_path)

    # 중복을 제거한 track spotify_id 리스트
    partition = math.ceil(len(df) / 3)
    start, end = idx * partition, (idx + 1) * partition
    partition_df = df.iloc[start : end]
    logging.info("start idx: " + str(start))
    logging.info("end idx: "+ str(end))

    spotify_track_id_list = list(set(partition_df['spotify_track_id'].to_list()))

    # 접근 토큰 가져오기
    access_token = get_access_token(spotify_client_id=SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=SPOTIFY_CLIENT_SECRETS[idx]) 

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"market": "US"}
    
    # 검색에 필요한 album_id와 artist_id 리스트에 삽입하기
    spotify_album_id_list = []
    spotify_artist_id_list = []
    
    tracks_dir = os.path.join(DOWNLOADS_DIR, f"spotify/api/tracks")
    os.makedirs(tracks_dir, exist_ok=True)  # exist_ok=True를 설정하면 디렉토리가 이미 존재할 경우 무시
        
    # 우선 market은 동일한 us로 설정(해당 market에서 사용 가능해야만 api 제공)
    # TODO: us에서도 못 듣는 track이라면 어떻게 해야될지 고민하기
    for spotify_track_id in spotify_track_id_list:
        
        response = requests.get(f"https://api.spotify.com/v1/tracks/{spotify_track_id}", 
                                params=params, headers=headers)
        
        # api 접근에 실패했다면 다시 접근
        while response.status_code != 200:
            access_token = get_access_token(spotify_client_id=SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=SPOTIFY_CLIENT_SECRETS[idx]) 
            
            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(f"https://api.spotify.com/v1/tracks/{spotify_track_id}", 
                params=params, headers=headers)
       
        track_json = response.json()
        
        # 앨범 id 추가
        spotify_album_id_list.append(track_json['album']['id'])

        for artist_json in track_json['artists']:
            spotify_artist_id_list.append(artist_json['id'])

        track_file_path = os.path.join(tracks_dir, f'{spotify_track_id}.json')
        with open(track_file_path, 'w') as f:
            json.dump(track_json, f, indent=4)

    spotify_album_id_list = list(set(spotify_album_id_list)) # 중복 제거
    spotify_artist_id_list = list(set(spotify_artist_id_list)) # 중복 제거

    context["ti"].xcom_push(key="spotify_album_id_list", value=spotify_album_id_list)
    context["ti"].xcom_push(key="spotify_artist_id_list", value=spotify_artist_id_list)

    logging.info("spotify_album_id_list length: " + str(len(spotify_album_id_list)))
    logging.info("spotify_artist_id_list length: " + str(len(spotify_artist_id_list)))
    


# snowflake에 있는지 확인할 album들에 list 모음 (merge into 실행하기)
def get_spotify_album_api(idx:int, **context) -> None:
    spotify_album_id_list = context["ti"].xcom_pull(key="spotify_album_id_list")
    partition = math.ceil(len(spotify_album_id_list) / 3)

    start, end = idx * partition, (idx + 1) * partition
    spotify_album_id_list = spotify_album_id_list[start:end]

    # 접근 토큰 가져오기
    access_token = get_access_token(spotify_client_id=SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=SPOTIFY_CLIENT_SECRETS[idx]) 

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"market": "US"}

    albums_dir = os.path.join(DOWNLOADS_DIR, f"spotify/api/albums")
    os.makedirs(albums_dir, exist_ok=True)  # exist_ok=True를 설정하면 디렉토리가 이미 존재할 경우 무시
    
    for spotify_album_id in spotify_album_id_list:
        access_token = get_access_token(spotify_client_id=SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=SPOTIFY_CLIENT_SECRETS[idx]) 
            
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(f"https://api.spotify.com/v1/albums/{spotify_album_id}", 
                        params=params, headers=headers)
        
        album_json = response.json()

        album_file_path = os.path.join(albums_dir, f'{spotify_album_id}.json')
        with open(album_file_path, 'w') as f:
            json.dump(album_json, f, indent=4)

# snowflake에 있는지 확인할 artist들에 list 모음 (merge into 실행하기)
def get_spotify_artist_api(idx:int, **context) -> None:
    spotify_artist_id_list = context["ti"].xcom_pull(key="spotify_artist_id_list")
    partition = math.ceil(len(spotify_artist_id_list) / 3)

    start, end = idx * partition, (idx + 1) * partition
    spotify_artist_id_list = spotify_artist_id_list[start:end]

    # 접근 토큰 가져오기
    access_token = get_access_token(spotify_client_id=SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=SPOTIFY_CLIENT_SECRETS[idx]) 

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"market": "US"}

    artists_dir = os.path.join(DOWNLOADS_DIR, f"spotify/api/artists")
    os.makedirs(artists_dir, exist_ok=True)  # exist_ok=True를 설정하면 디렉토리가 이미 존재할 경우 무시
    
    for spotify_artist_id in spotify_artist_id_list:
        access_token = get_access_token(spotify_client_id=SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=SPOTIFY_CLIENT_SECRETS[idx]) 
            
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(f"https://api.spotify.com/v1/artists/{spotify_artist_id}", 
                        params=params, headers=headers)
        
        artist_json = response.json()

        artist_file_path = os.path.join(artists_dir, f'{spotify_artist_id}.json')
        with open(artist_file_path, 'w') as f:
            json.dump(artist_json, f, indent=4)


# TODO: api S3에 담는거 추가하기
def load_spotify_api_to_s3(src_path: str, bucket_name: str) -> None:
    src_files_path = os.path.join(DOWNLOADS_DIR, src_path)

    filenames = glob.glob(src_files_path)
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]
    logging.info(filenames[0])
    logging.info(keys[0])
    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

# TODO: S3에 담은 후에 삭제하는 거 추가하기
    


with DAG(dag_id="spotify_api_dag",
         schedule_interval=None, # trigger DAG는 보통 None으로 처리 합니다.
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )
    
    with TaskGroup("spotify_track_group") as spotify_track_group:
        for i in range(3):
            spotify_track_api_task = PythonOperator(
                task_id=f"spotify_track_api_task_{i+1}",
                python_callable=get_spotify_track_api,
                op_kwargs={
                    "src_path": f"spotify/charts/{NOW_DATE}/transform-concat-daily-{NOW_DATE}.csv",
                    "idx": i
                }
            )

    load_track_to_s3_task = PythonOperator(
        task_id="load_track_to_s3_task",
        python_callable=load_spotify_api_to_s3,
        op_kwargs={
            "src_path": f'spotify/api/tracks/*.json',
            "bucket_name": BUCKET_NAME
        }
    )
 
    with TaskGroup("spotify_album_group") as spotify_album_group:
        for i in range(3):
            spotify_album_api_task = PythonOperator(
                task_id=f"spotify_album_api_task_{i+1}",
                python_callable=get_spotify_album_api,
                op_kwargs={
                    "idx": i
                }
            )

    load_album_to_s3_task = PythonOperator(
        task_id="load_album_to_s3_task",
        python_callable=load_spotify_api_to_s3,
        op_kwargs={
            "src_path": f'spotify/api/albums/*.json',
            "bucket_name": BUCKET_NAME
        }
    )

    with TaskGroup("spotify_artist_group") as spotify_artist_group:
        for i in range(3):
            spotify_album_api_task = PythonOperator(
                task_id=f"spotify_artist_api_task_{i+1}",
                python_callable=get_spotify_artist_api,
                op_kwargs={
                    "idx": i
                }
            )

    load_artist_to_s3_task = PythonOperator(
        task_id="load_artist_to_s3_task",
        python_callable=load_spotify_api_to_s3,
        op_kwargs={
            "src_path": f'spotify/api/artists/*.json',
            "bucket_name": BUCKET_NAME
        }
    )

    # get_spotify_track_api_task = PythonOperator(
    #     task_id="get_spotify_track_api_task",
    #     python_callable=get_spotify_track_api,
    #     op_kwargs={
    #         "src_path": f"spotify/charts/{NOW_DATE}/transform-concat-daily-{NOW_DATE}.csv"
    #     }
    # )

    # get_spotify_album_api_task = PythonOperator(
    #     task_id="get_spotify_album_api_task",
    #     python_callable=get_spotify_album_api
    # )

    # get_spotify_artist_api_task = PythonOperator(
    #     task_id="get_spotify_artist_api_task",
    #     python_callable=get_spotify_artist_api
    # )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> spotify_track_group >> load_track_to_s3_task
    spotify_track_group >> spotify_album_group 

    spotify_album_group >> load_album_to_s3_task
    spotify_album_group >> spotify_artist_group

    spotify_artist_group >> load_artist_to_s3_task
    load_artist_to_s3_task >> end_task