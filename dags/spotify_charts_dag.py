from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
import time

from datetime import datetime, timedelta
import pandas as pd
from typing import List, Tuple
import logging
import os
import pendulum
import glob
from utils.constant_util import *
from utils import common_util

def get_spotify_chart_urls() -> List:
    base_url = "https://charts.spotify.com/charts/view/"
    spotify_charts_urls = [base_url + f'regional-{region}-daily/{US_DATE}' for region in REGIONS]

    logging.info(spotify_charts_urls[0])
    return spotify_charts_urls


def spotify_charts_csv() -> None:
    src_path = os.path.join(DOWNLOADS_DIR, f'spotify/charts/{US_DATE}')

    if not os.path.exists(src_path):
        os.makedirs(src_path)

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": src_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })  

    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    driver.get('https://charts.spotify.com/charts/view/regional-ar-daily/2024-03-14')
    spotify_chart_urls = get_spotify_chart_urls()

    time.sleep(2)
    # 로그인 버튼
    driver.find_element(By.CLASS_NAME, 'ButtonInner-sc-14ud5tc-0.iMWZgy.encore-bright-accent-set').click()

    time.sleep(3)
    # ID, PW 입력
    username_field = driver.find_element(By.ID, 'login-username') # 예시 id, 실제 id로 대체해야 함
    username_field.send_keys(SPOTIFY_CHARTS_LOGIN_USERNAME)

    password_field = driver.find_element(By.ID, 'login-password') # 예시 id, 실제 id로 대체해야 함
    password_field.send_keys(SPOTIFY_CHARTS_LOGIN_PASSWORD)

    login_button = driver.find_element(By.ID, 'login-button') # 예시 id, 실제 id로 대체해야 함
    login_button.click()

    time.sleep(3)
    
    for url in spotify_chart_urls[:5]:
        # daily_address = address + '/' + US_DATE
        driver.get(url)

        time.sleep(5)
        # 쿠키 삭제
        try:
            driver.find_element(By.CLASS_NAME, 'onetrust-close-btn-handler.onetrust-close-btn-ui.banner-close-button.ot-close-icon').click()
        except:
            pass

        time.sleep(2)
        # 다운로드 버튼 클릭
        driver.find_element(By.CLASS_NAME, 'styled__CSVLink-sc-135veyd-5.kMpXks').click()

        time.sleep(5)

    driver.quit()


def load_spotify_charts_to_s3(bucket_name: str) -> None:
    src_path = os.path.join(DOWNLOADS_DIR, f'spotify/charts/{US_DATE}/*.csv')

    filenames = glob.glob(src_path)
    keys = [filename.replace(AIRFLOW_HOME, "")[1:] for filename in filenames]
    logging.info(filenames[0])
    logging.info(keys[0])

    common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

with DAG(dag_id="spotify_charts_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    spotify_charts_csv_task = PythonOperator(
        task_id="urls_task",
        python_callable=spotify_charts_csv
    )
    
    load_spotify_charts_to_s3_task = PythonOperator(
        task_id = "load_spotify_charts_to_s3_task",
        python_callable= load_spotify_charts_to_s3,
        op_kwargs= {
            "bucket_name": BUCKET_NAME
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

    start_task >> spotify_charts_csv_task >> load_spotify_charts_to_s3_task >> end_task