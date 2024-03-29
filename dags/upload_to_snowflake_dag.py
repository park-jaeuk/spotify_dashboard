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

from sql import album_sql, artist_sql, track_sql, track_chart_sql

def upload_to_s3(filename: str, key: str, bucket_name: str, replace: bool) -> None:
    hook = S3Hook("aws_s3")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=replace)


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
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    load_album_task = SnowflakeOperator(
        task_id='load_album_task',
        sql=album_sql.sql,
        snowflake_conn_id='snowflake_default',
    )

    load_artist_task = SnowflakeOperator(
        task_id='load_artist_task',
        sql=artist_sql.sql,
        snowflake_conn_id='snowflake_default',
    )

    load_track_task = SnowflakeOperator(
        task_id='load_track_task',
        sql=track_sql.sql,
        snowflake_conn_id='snowflake_default'
    )

    load_track_chart_task = SnowflakeOperator(
        task_id='load_track_chart_task',
        sql=track_chart_sql.sql,
        snowflake_conn_id='snowflake_default'
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    
    start_task >> [load_album_task, load_artist_task, load_track_task] >> load_track_chart_task
    load_track_chart_task >> end_task