from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime
import pandas as pd
from typing import List, Tuple

from sql import url
from utils import common_util
from utils.constant_util import Directory, Config, Date

from sql import album
from sql import artist
from sql import information
from sql import review
from sql import tag
from sql import track_artist
from sql import track_chart
from sql import track

with DAG(dag_id="upload_last_fm_to_snowflake_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    load_information_task = SnowflakeOperator(
        task_id='load_information_task',
        sql=information.select_information(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    load_review_task = SnowflakeOperator(
        task_id='load_review_task',
        sql=review.select_review(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    load_tag_task = SnowflakeOperator(
        task_id='load_tag_task',
        sql=tag.select_tag(Config.BUCKET_NAME, Date.US_DATE),
        snowflake_conn_id='s3_to_snowflake'
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> [load_information_task, load_review_task, load_tag_task] >> end_task