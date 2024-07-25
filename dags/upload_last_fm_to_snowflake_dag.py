from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

    trigger_delete_dirs_task = TriggerDagRunOperator(
        task_id='trigger_delete_dirs_task',
        trigger_dag_id='delete_dirs_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

    trigger_setting_date_task = TriggerDagRunOperator(
        task_id='trigger_setting_date_task',
        trigger_dag_id='setting_date_dag',
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

    start_task >> [load_information_task, load_review_task, load_tag_task] >> trigger_delete_dirs_task >> trigger_setting_date_task >> end_task