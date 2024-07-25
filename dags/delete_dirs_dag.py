from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
from typing import List, Tuple
from utils import common_util
from utils.constant_util import Directory, Config
import shutil
import os

def delete_dirs(directories: List):
    for directory in directories:
        shutil.rmtree(directory)

with DAG(dag_id="delete_dirs_dag",
         schedule_interval=None,
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    directories = []
    directories.append(os.path.join(Directory.DOWNLOADS_DIR, f'spotify/api'))
    directories.append(os.path.join(Directory.DOWNLOADS_DIR, f'last_fm'))
    
    directories.append(os.path.join(Directory.TRANSFORM_DIR, f'spotify'))
    directories.append(os.path.join(Directory.TRANSFORM_DIR, f'last_fm'))

    start_task = EmptyOperator(
        task_id = "start_task"
    )

    delete_dirs_task = PythonOperator(
        task_id="delete_dirs_task",
        python_callable=delete_dirs,
        op_kwargs={
            "directories": directories
        }
    )

    end_task = EmptyOperator(
        task_id = "end_task"
    )

    start_task >> delete_dirs_task >> end_task