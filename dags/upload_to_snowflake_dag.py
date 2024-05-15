from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import pandas as pd
from typing import List, Tuple
from utils.constant_util import Directory, Config, Date

from sql import album
from sql import artist
from sql import information
from sql import review
from sql import tag
from sql import track_artist
from sql import track_chart
from sql import track


with DAG(dag_id="upload_to_snowflake_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    load_album_task = SnowflakeOperator(
        task_id='load_album_task',
        sql=album.select_album(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_artist_task = SnowflakeOperator(
        task_id='load_artist_task',
        sql=artist.select_artist(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_information_task = SnowflakeOperator(
        task_id='load_information_task',
        sql=information.select_information(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_review_task = SnowflakeOperator(
        task_id='load_review_task',
        sql=review.select_review(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_tag_task = SnowflakeOperator(
        task_id='load_tag_task',
        sql=tag.select_tag(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_track_artist_task = SnowflakeOperator(
        task_id='load_track_artist_task',
        sql=track_artist.select_track_artist(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_track_chart_task = SnowflakeOperator(
        task_id='load_track_chart_task',
        sql=track_chart.select_track_chart(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    load_track_task = SnowflakeOperator(
        task_id='load_track_task',
        sql=track.select_track(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake',
    )

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger_task',
        trigger_dag_id='delete_dirs_dag',
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

    start_task >> [load_album_task, load_artist_task, load_information_task,
                   load_review_task, load_tag_task, load_track_artist_task, 
                   load_track_chart_task, load_track_task] >> call_trigger_task
    
    
    call_trigger_task >> end_task