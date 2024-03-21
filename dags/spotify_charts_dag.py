from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable

from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import pendulum
import glob

# TODO : selenium 크롤링 구현하기
# def get_csv_files()

def upload_raw_files_to_s3(bucket_name: str) -> None:
    hook = S3Hook(aws_conn_id="aws_s3")
    src_path = os.path.join(RAW_DATA_DIR, f'spotify/charts/{NOW_DATE}/*.csv')

    filenames = glob.glob(src_path)
    logging.info(filenames[0])

    for filename in filenames:
        key = filename.replace(RAW_DATA_DIR, '')
        key = os.path.join('raw_data', key[1:])
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

def transform_and_concat_df(src_dir_path:str, dst_dir_path:str) -> None:
    logging.info(src_dir_path + dst_dir_path)
    logging.info("transform and concat DataFrame")
    # 트랙 테이블에서 spotify_id(외부 아이디)로 id 값을 가져와서 track_info 연결하기
 
    src_dir_path = os.path.join(RAW_DATA_DIR, f'spotify/charts/{NOW_DATE}')
    src_files = os.path.join(src_dir_path, "*.csv")


    dst_dir_path = os.path.join(TRANSFORM_DIR, f'spotify/charts/{NOW_DATE}')
        
    if not os.path.exists(dst_dir_path):
        os.makedirs(dst_dir_path)

    filenames = glob.glob(src_files)
    
    save_columns = ['spotify_id', 'artist_names', 'track_name', 'now_rank', 'peak_rank', 'previous_rank', 
                'total_days_on_chart', 'stream_count', 'region', 'chart_date']

    concat_df = pd.DataFrame(columns = save_columns)

    for filename in filenames:
        df = pd.read_csv(filename)
        
        # 데이터 변환 
        df['spotify_id'] = df['uri'].str.split(':').str[-1]
        df['chart_date'] = NOW_DATE
        df['region'] = filename.split('/')[-1].split('-')[1] 

        # 컬럼 이름 변경
        rename_columns = {'rank' : 'now_rank', 'streams':'stream_count', 'days_on_chart':'total_days_on_chart'}
        df.rename(columns = rename_columns, inplace = True)

        # 필요없는 삭제 및 컬럼 순서 변경
        df = df[save_columns]


        logging.info(len(df))

        concat_df = pd.concat([concat_df, df])

    dst_file = os.path.join(dst_dir_path, f'transform-concat-daily-{NOW_DATE}.csv')
    concat_df.to_csv(dst_file, index=False)

    logging.info(dst_file)
    logging.info(len(concat_df))


# TODO: charts S3에 담는거 추가하기



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

with DAG(dag_id="spotify_charts_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )
    
    upload_raw_files_to_s3_task = PythonOperator(
        task_id = "upload_raw_files_to_s3_task",
        python_callable= upload_raw_files_to_s3,
        op_kwargs= {
            "bucket_name": BUCKET_NAME
        }
    )

    transform_and_concat_csv_task = PythonOperator(
        task_id = "transform_csv_task",
        python_callable=transform_and_concat_df,
        op_kwargs={
            "dst_dir_path": "This is dst",
            "src_dir_path": "This is src", 
        }
    )

    # spotify_api_dag trigger
    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='spotify_api_dag',
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

    # start_task >> upload_raw_files_to_s3_task >> end_task

    # start_task >> transform_and_concat_csv_task >> call_trigger_task >> end_task

    start_task >> transform_and_concat_csv_task >> end_task