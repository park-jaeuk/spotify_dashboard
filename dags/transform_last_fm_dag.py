from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.variable import Variable

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import glob
import json
import numpy as np

from sql import url
from utils import common_util
from utils.constant_util import Directory, Config, Date


def transform_reviews_csv() -> None:
    review_dict = {}
    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'last_fm/reviews')

    for review_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(review_json_path, "r", encoding='UTF-8') as review_json:
            review_api = json.load(review_json)

        columns = ['spotify_track_id', 'review', 'date', 'likes']
        review_dict = {column : [] for column in columns}
        spotify_track_id = list(review_api.keys())[0]
        reviews = review_api[spotify_track_id]
        for review in reviews:
            review_dict['spotify_track_id'].append(spotify_track_id)
            review_dict['review'].append(review['review'])
            review_dict['date'].append(review['date'])
            review_dict['likes'].append(review['likes'])
            review_df = pd.DataFrame(review_dict, columns = columns).drop_duplicates()
        dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, 'last_fm/reviews')
        os.makedirs(dst_dir_path, exist_ok=True)
        dst_file_path = os.path.join(dst_dir_path, f"{spotify_track_id}.csv")
        review_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

def transform_information_tag_csv() -> None:
    columns = ['listeners','length', 'genres', 'last_fm_url', 'introduction', 'spotify_track_id']
    src_dir_path = os.path.join(Directory.DOWNLOADS_DIR, f'last_fm/information')
    transform_df = pd.DataFrame(columns = columns)

    for info_json_path in glob.glob(os.path.join(src_dir_path, "*.json")):
        with open(info_json_path, "r", encoding='UTF-8') as info_json:
            info_api = json.load(info_json)
            
        spotify_track_id = info_json_path.split('/')[-1][:-5]
        info_api['spotify_track_id'] = spotify_track_id
        try:
            tmp = pd.DataFrame(info_api)
        except:
            tmp = pd.DataFrame([info_api])
        transform_df = pd.concat([transform_df, tmp], axis = 0)
    transform_df = transform_df.drop_duplicates() 
    transform_df.reset_index(drop=True, inplace=True)

    info_df = transform_df[['spotify_track_id' , 'listeners', 'length', 'last_fm_url', 'introduction']]
    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'last_fm/information/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"total_information.csv")
    info_df = info_df.drop_duplicates() 
    info_df.reset_index(drop=True, inplace=True)
    info_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)

    tag_df = transform_df[['spotify_track_id', 'genres']]
    tag_df.columns = ['spotify_track_id', 'tags']
    tag_df['tags'].replace('', np.nan, inplace=True)
    tag_df.dropna(subset=['tags'])
    dst_dir_path = os.path.join(Directory.TRANSFORM_DIR, f'last_fm/tags/{Date.US_DATE}')
    os.makedirs(dst_dir_path, exist_ok=True)
    dst_file_path = os.path.join(dst_dir_path, f"total_tags.csv")
    tag_df = tag_df.drop_duplicates() 
    tag_df.reset_index(drop=True, inplace=True)
    tag_df.to_csv(dst_file_path, encoding='utf-8-sig',index=False)



def upload_transform_reviews_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR,'last_fm/reviews/*.csv')
    filenames = glob.glob(src_path)
    keys = [filename.replace(Directory.AIRFLOW_HOME, "")[1:] for filename in filenames]
    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

def upload_transform_information_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'last_fm/information/{Date.US_DATE}/*.csv')
    filename = glob.glob(src_path)[0]
    key = filename.replace(Directory.AIRFLOW_HOME, "")[1:]
    common_util.upload_file_to_s3(filename=filename, key=key, bucket_name=bucket_name, replace=True)

def upload_transform_tags_csv_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(Directory.TRANSFORM_DIR, f'last_fm/tags/{Date.US_DATE}/*.csv')
    filename = glob.glob(src_path)[0]
    key = filename.replace(Directory.AIRFLOW_HOME, "")[1:]

    Variable.delete("shared_list")
    common_util.upload_file_to_s3(filename=filename, key=key, bucket_name=bucket_name, replace=True)


with DAG(dag_id="transform_last_fm_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    transform_reviews_csv_task = PythonOperator(
        task_id = "transform_reviews_csv_task",
        python_callable=transform_reviews_csv
    )

    transform_information_tag_csv_task = PythonOperator(
        task_id = "transform_information_tag_csv_task",
        python_callable=transform_information_tag_csv
    )

    upload_transform_reviews_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_review_csv_to_s3_task",
        python_callable=upload_transform_reviews_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    upload_transform_tags_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_tags_csv_to_s3_task",
        python_callable=upload_transform_tags_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    upload_transform_information_csv_to_s3_task = PythonOperator(
        task_id="upload_transform_information_csv_to_s3_task",
        python_callable=upload_transform_information_csv_to_s3,
        op_kwargs= {
            "bucket_name": Config.BUCKET_NAME
        }
    )

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='upload_last_fm_to_snowflake_dag',
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

    
    
    start_task >> [transform_reviews_csv_task, transform_information_tag_csv_task]

    transform_reviews_csv_task >> upload_transform_reviews_csv_to_s3_task
    transform_information_tag_csv_task >> [upload_transform_information_csv_to_s3_task, upload_transform_tags_csv_to_s3_task]
    
    [upload_transform_reviews_csv_to_s3_task, upload_transform_information_csv_to_s3_task, upload_transform_tags_csv_to_s3_task] >> call_trigger_task >> end_task