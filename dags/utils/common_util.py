import os
import logging
from typing import List
import pandas as pd


from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_newest_date(**kwargs):
    ti = kwargs['ti']
    select_track_list = ti.xcom_pull(task_ids='select_last_fm_task')
    df  = pd.DataFrame(select_track_list)
    logging.info(df.columns)

    df['TRACK'] = df['TRACK'].str.replace(' ','+')
    df['ARTIST'] = df['ARTIST'].str.replace(' ','+')

    basic_url = 'https://www.last.fm/music/'
    url_list = basic_url + df['ARTIST'] + '/_/' + df['TRACK'] + '/+shoutbox?sort=newest&page='

    lst = list(zip(list(df['SPOTIFY_TRACK_ID']), df['MAX_REVIEWS_DATE']))
    dic = dict(zip(url_list,lst))
    return dic



def save_file(df: pd.DataFrame, target_dir: str, target_file: str) -> None:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        target_dir (str): _description_
        target_file (str): _description_
    """
    save_file_path = os.path.join(target_dir, target_file)
    df.to_csv(save_file_path, index=False) 

def delete_file(df: pd.DataFrame, target_dir: str, target_file: str) -> None:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        target_dir (str): _description_
        target_file (str): _description_
    """
    delete_file_path = os.path.join(target_dir, target_file)
    os.remove(delete_file_path)

def upload_file_to_s3(filename: str, key: str, bucket_name: str, replace: bool) -> None:
    """ Airflow Hook으로 S3에 하나 파일을 업로드하는 메서드

    Args:
        filename (str): 저장할 로컬 파일 디렉토리
        key (str): S3의 저장할 파일 디렉토리
        bucket_name (str): S3 버켓 이름
        replace (bool): 덮어쓰기 할 건지 O, X
    """
    hook = S3Hook("aws_s3")
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=replace)

def upload_files_to_s3(filenames: List, keys: List, bucket_name: str, replace: bool) -> None:
    """ Airflow Hook으로 S3에 여러 파일을 업로드하는 메서드

    Args:
        filenames (List): 저장할 로컬 파일 디렉토리 리스트
        keys (List): S3의 저장할 파일 디렉토리 리스트
        bucket_name (str): S3 버켓 이름
        replace (bool): 덮어쓰기 할 건지 O, X
    """
    hook = S3Hook("aws_s3")

    for filename, key in zip(filenames, keys):
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=replace)


def get_new_keys(bucket_name: str, table: str):
    """S3 버킷에서 특정 경로의 파일 목록을 가져옵니다."""
    # S3Hook 인스턴스 생성, Airflow Connection ID 지정
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    
    # 버킷 내 파일 목록 조회
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=f'downloads/spotify/api/{table}')
    s3_keys = [file.split('/')[-1][:-5] for file in files[1:]]
    return s3_keys