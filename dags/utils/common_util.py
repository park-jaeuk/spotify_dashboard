import os
import logging
from typing import List
import pandas as pd


from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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