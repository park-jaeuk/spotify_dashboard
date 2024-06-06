from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime
import pandas as pd
from typing import List, Tuple
from utils import constant_util

from sql import album
from sql import artist
from sql import information
from sql import review
from sql import tag
from sql import track_artist
from sql import track_chart
from sql import track


with DAG(dag_id="upload_spotify_to_snowflake_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    load_album_task = SnowflakeOperator(
        task_id='load_album_task',
        sql=album.select_album(constant_util.BUCKET_NAME, constant_util.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    load_artist_task = SnowflakeOperator(
        task_id='load_artist_task',
        sql=artist.select_artist(constant_util.BUCKET_NAME, constant_util.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )


    load_track_artist_task = SnowflakeOperator(
        task_id='load_track_artist_task',
        sql=track_artist.select_track_artist(constant_util.BUCKET_NAME, constant_util.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    load_track_chart_task = SnowflakeOperator(
        task_id='load_track_chart_task',
        sql=track_chart.select_track_chart(constant_util.BUCKET_NAME, constant_util.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    load_track_task = SnowflakeOperator(
        task_id='load_track_task',
        sql=track.select_track(constant_util.BUCKET_NAME, constant_util.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> [load_album_task, load_artist_task, load_track_artist_task, load_track_chart_task, load_track_task] >> end_task