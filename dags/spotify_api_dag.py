from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import base64
import requests
import glob
import json
import math
import time
from utils import constant_util
from utils import common_util



def get_access_token(spotify_client_id: str, spotify_client_secret: str) -> str:
    endpoint = 'https://accounts.spotify.com/api/token'

    encoded = base64.b64encode(f'{spotify_client_id}:{spotify_client_secret}'.encode('utf-8')).decode('ascii')

    headers = {'Authorization': f'Basic {encoded}',
               'Content-Type': 'application/x-www-form-urlencoded'
              }
    payload = {'grant_type': 'client_credentials'}

    response = ""
    while response == "":
        try :
            response = requests.post(endpoint, data=payload, headers=headers)
        except :
            print("Connection refused by the server, sleep for 5 seconds")
            time.sleep(5)
            continue

    access_token = response.json()['access_token']
    
    return access_token

# snowflake에 있는지 확인할 track들에 list 모음 (merge into 실행하기)
def get_spotify_track_api(src_path: str, num_partition: int, idx:int, **context) -> None:
    csv_path = os.path.join(constant_util.TRANSFORM_DIR, src_path)
    df = pd.read_csv(csv_path)

    # 분할한 task에 대한 파티션
    partition = math.ceil(len(df) / num_partition)
    start, end = idx * partition, (idx + 1) * partition
    partition_df = df.iloc[start : end]
    logging.info("start idx: " + str(start))
    logging.info("end idx: "+ str(end))

    # 중복을 제거한 track spotify_id 리스트
    spotify_track_id_list = list(set(partition_df['spotify_track_id'].to_list()))

    spotify_track_id_list = list(set(spotify_track_id_list) - set(common_util.get_new_keys(constant_util.BUCKET_NAME, 'tracks')))
    logging.info(f"the number of new track : {len(spotify_track_id_list)}")

    # 접근 토큰 가져오기
    access_token = get_access_token(spotify_client_id=constant_util.SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=constant_util.SPOTIFY_CLIENT_SECRETS[idx]) 

    
    # 검색에 필요한 album_id와 artist_id 리스트에 삽입하기
    spotify_album_id_list = []
    spotify_artist_id_list = []
    
    tracks_dir = os.path.join(constant_util.DOWNLOADS_DIR, f"spotify/api/tracks")
    os.makedirs(tracks_dir, exist_ok=True)  # exist_ok=True를 설정하면 디렉토리가 이미 존재할 경우 무시
    
    # 우선 market은 동일한 us로 설정(해당 market에서 사용 가능해야만 api 제공)
    # TODO: us에서도 못 듣는 track이라면 어떻게 해야될지 고민하기
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"market": "US"}

    batch_size = math.ceil(len(spotify_track_id_list) / 20)

    
    for batch in range(batch_size):
        partition_track_id_list = spotify_track_id_list[batch * 20 : (batch + 1) * 20]
    
        track_ids = ",".join(partition_track_id_list)

        response = ""

        while response == "":
            try :
                response = requests.get(f"https://api.spotify.com/v1/tracks?ids={track_ids}", 
                                    params=params, headers=headers)
            except :
                print("Connection refused by the server, sleep for 5 seconds")
                time.sleep(5)
                continue

        
        track_jsons = response.json()['tracks']

        for track_json in track_jsons:
            spotify_track_id = track_json['id']
            
            track_file_path = os.path.join(tracks_dir, f'{spotify_track_id}.json')
            
            # 앨범 id 추가
            spotify_album_id_list.append(track_json['album']['id'])

            # 아티스트 id 추가
            for artist_json in track_json['artists']:
                spotify_artist_id_list.append(artist_json['id'])


            with open(track_file_path, 'w') as f:
                json.dump(track_json, f, indent=4)
        
    context["ti"].xcom_push(key=f"spotify_album_id_list_{idx}", value=spotify_album_id_list)
    context["ti"].xcom_push(key=f"spotify_artist_id_list_{idx}", value=spotify_artist_id_list)


def get_id_list(num_partition: int, **context) -> None:
    spotify_album_id_list = []
    spotify_artist_id_list = []

    for idx in range(num_partition):
        logging.info("album, artist")
        logging.info(context['ti'].xcom_pull(key=f"spotify_album_id_list_{idx}"))
        logging.info(context['ti'].xcom_pull(key=f"spotify_artist_id_list_{idx}"))
        

        spotify_album_id_list.extend(
            context["ti"].xcom_pull(key=f"spotify_album_id_list_{idx}")
        )

        spotify_artist_id_list.extend(
            context["ti"].xcom_pull(key=f"spotify_artist_id_list_{idx}")
        )    

    spotify_album_id_list = list(set(spotify_album_id_list) - set(common_util.get_new_keys(constant_util.BUCKET_NAME, 'albums'))) # 중복 제거
    spotify_artist_id_list = list(set(spotify_artist_id_list) - set(common_util.get_new_keys(constant_util.BUCKET_NAME, 'artists'))) # 중복 제거
    logging.info(f'the number of new album : f{spotify_album_id_list}')
    logging.info(f'the number of new artist : f{spotify_artist_id_list}')

    context["ti"].xcom_push(key=f"spotify_album_id_list", value=spotify_album_id_list)
    context["ti"].xcom_push(key=f"spotify_artist_id_list", value=spotify_artist_id_list)

    logging.info("spotify_album_id_list length: " + str(len(spotify_album_id_list)))
    logging.info("spotify_artist_id_list length: " + str(len(spotify_artist_id_list)))

# snowflake에 있는지 확인할 album들에 list 모음 (merge into 실행하기)
def get_spotify_album_api(num_partition: int, idx:int, **context) -> None:
    spotify_album_id_list = context["ti"].xcom_pull(key="spotify_album_id_list")
    partition = math.ceil(len(spotify_album_id_list) / num_partition)

    start, end = idx * partition, (idx + 1) * partition
    spotify_album_id_list = spotify_album_id_list[start:end]
    logging.info("start: " + str(start))
    logging.info("end: " + str(end))

    # 접근 토큰 가져오기
    access_token = get_access_token(spotify_client_id=constant_util.SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=constant_util.SPOTIFY_CLIENT_SECRETS[idx]) 

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"market": "US"}

    albums_dir = os.path.join(constant_util.DOWNLOADS_DIR, f"spotify/api/albums")
    os.makedirs(albums_dir, exist_ok=True)  # exist_ok=True를 설정하면 디렉토리가 이미 존재할 경우 무시
    
    batch_size = math.ceil(len(spotify_album_id_list) / 20)
    
    for batch in range(batch_size):
        partition_album_id_list = spotify_album_id_list[batch * 20 : (batch + 1) * 20]
    
        album_ids = ",".join(partition_album_id_list)

        response = ""

        while response == "":
            try :
                response = requests.get(f"https://api.spotify.com/v1/albums?ids={album_ids}", 
                                    params=params, headers=headers)
            except :
                print("Connection refused by the server, sleep for 5 seconds")
                time.sleep(5)
                continue
        
        album_jsons = response.json()['albums']

        for album_json, album_id in zip(album_jsons, album_ids):
            
            while 'error' in album_json.keys():
                try :
                    response = requests.get(f"https://api.spotify.com/v1/albums/{album_id}", 
                                    params=params, headers=headers)
                    album_json = response.json()
                except :
                    print("Connection refused by the server, sleep for 5 seconds")
                    time.sleep(5)
                    continue

            spotify_album_id = album_json['id']
            
            album_file_path = os.path.join(albums_dir, f'{spotify_album_id}.json')
            
            with open(album_file_path, 'w') as f:
                json.dump(album_json, f, indent=4)
    
# snowflake에 있는지 확인할 artist들에 list 모음 (merge into 실행하기)
def get_spotify_artist_api(num_partition: int, idx:int, **context) -> None:
    spotify_artist_id_list = context["ti"].xcom_pull(key="spotify_artist_id_list")
    partition = math.ceil(len(spotify_artist_id_list) / num_partition)

    start, end = idx * partition, (idx + 1) * partition
    spotify_artist_id_list = spotify_artist_id_list[start:end]
    logging.info("start :", start)
    logging.info("end :", end)
    
    # 접근 토큰 가져오기
    access_token = get_access_token(spotify_client_id=constant_util.SPOTIFY_CLIENT_IDS[idx], 
                                    spotify_client_secret=constant_util.SPOTIFY_CLIENT_SECRETS[idx]) 

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"market": "US"}

    artists_dir = os.path.join(constant_util.DOWNLOADS_DIR, f"spotify/api/artists")
    os.makedirs(artists_dir, exist_ok=True)  # exist_ok=True를 설정하면 디렉토리가 이미 존재할 경우 무시
    
    batch_size = math.ceil(len(spotify_artist_id_list) / 20)
    
    for batch in range(batch_size):
        partition_artist_id_list = spotify_artist_id_list[batch * 20 : (batch + 1) * 20]
    
        artist_ids = ",".join(partition_artist_id_list)

        response = ""

        while response == "":
            try :
                response = requests.get(f"https://api.spotify.com/v1/artists?ids={artist_ids}", 
                                    params=params, headers=headers)
            except :
                print("Connection refused by the server, sleep for 5 seconds")
                time.sleep(5)
                continue
        
        artist_jsons = response.json()['artists']

        for artist_json, artist_id in zip(artist_jsons, artist_ids):
            
            while 'error' in artist_json.keys():
                try :
                    response = requests.get(f"https://api.spotify.com/v1/artists/{artist_id}", 
                                    params=params, headers=headers)
                    artist_json = response.json()
                except :
                    print("Connection refused by the server, sleep for 5 seconds")
                    time.sleep(5)
                    continue

            spotify_artist_id = artist_json['id']
            
            artist_file_path = os.path.join(artists_dir, f'{spotify_artist_id}.json')
            
            with open(artist_file_path, 'w') as f:
                json.dump(artist_json, f, indent=4)

def load_spotify_api_to_s3(src_path: str, bucket_name: str) -> None:
    src_files_path = os.path.join(constant_util.DOWNLOADS_DIR, src_path)

    filenames = glob.glob(src_files_path)
    keys = [filename.replace(constant_util.AIRFLOW_HOME, "")[1:] for filename in filenames]
    logging.info(filenames[0])
    logging.info(keys[0])
    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

# TODO: 현재 S3에 존재하는지 

# TODO: S3에 담은 후에 삭제하는 거 추가하기
    
NUM_PARTITION = 3

with DAG(dag_id="spotify_api_dag",
         schedule_interval=None, # trigger DAG는 보통 None으로 처리 합니다.
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )
    
    with TaskGroup("spotify_track_group") as spotify_track_group:
        for i in range(NUM_PARTITION):
            spotify_track_api_task = PythonOperator(
                task_id=f"spotify_track_api_task_{i+1}",
                python_callable=get_spotify_track_api,
                op_kwargs={
                    "num_partition": NUM_PARTITION,
                    "src_path": f"spotify/charts/{constant_util.US_DATE}/transform-concat-daily-{constant_util.US_DATE}.csv",
                    "idx": i
                }
            )

    load_track_to_s3_task = PythonOperator(
        task_id="load_track_to_s3_task",
        python_callable=load_spotify_api_to_s3,
        op_kwargs={
            "src_path": f'spotify/api/tracks/*.json',
            "bucket_name": constant_util.BUCKET_NAME
        }
    )
 
    with TaskGroup("spotify_album_group") as spotify_album_group:
        for i in range(NUM_PARTITION):
            spotify_album_api_task = PythonOperator(
                task_id=f"spotify_album_api_task_{i+1}",
                python_callable=get_spotify_album_api,
                op_kwargs={
                    "num_partition": NUM_PARTITION,
                    "idx": i
                }
            )

    get_id_list_task = PythonOperator(
        task_id = "get_id_list_task",
        python_callable=get_id_list,
        op_kwargs={
            "num_partition": NUM_PARTITION,
        }
    )
    

    load_album_to_s3_task = PythonOperator(
        task_id="load_album_to_s3_task",
        python_callable=load_spotify_api_to_s3,
        op_kwargs={
            "src_path": f'spotify/api/albums/*.json',
            "bucket_name": constant_util.BUCKET_NAME
        }
    )

    with TaskGroup("spotify_artist_group") as spotify_artist_group:
        for i in range(NUM_PARTITION):
            spotify_album_api_task = PythonOperator(
                task_id=f"spotify_artist_api_task_{i+1}",
                python_callable=get_spotify_artist_api,
                op_kwargs={
                    "num_partition": NUM_PARTITION,
                    "idx": i
                }
            )

    load_artist_to_s3_task = PythonOperator(
        task_id="load_artist_to_s3_task",
        python_callable=load_spotify_api_to_s3,
        op_kwargs={
            "src_path": f'spotify/api/artists/*.json',
            "bucket_name": constant_util.BUCKET_NAME
        }
    )

    transform_spotify_trigger_task = TriggerDagRunOperator(
        task_id='transform_spotify_trigger_task',
        trigger_dag_id='transform_spotify_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

    last_fm_trigger_task = TriggerDagRunOperator(
        task_id='last_fm_trigger_task',
        trigger_dag_id='last_fm_dag',
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



    start_task >> spotify_track_group >> load_track_to_s3_task
    spotify_track_group >> get_id_list_task >>spotify_album_group 

    spotify_album_group >> load_album_to_s3_task
    spotify_album_group >> spotify_artist_group

    spotify_artist_group >> load_artist_to_s3_task >> [transform_spotify_trigger_task, last_fm_trigger_task] >> end_task