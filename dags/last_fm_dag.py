from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable
from bs4 import BeautifulSoup 
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import boto3
from selenium import webdriver
import os
import time
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import pandas as pd
from typing import List, Tuple
import logging
import os
import pendulum
import glob
import requests
import json
from urllib.parse import quote
from datetime import datetime

from sql import album
from sql import artist
from sql import track
from sql import url
from utils import common_util
from utils import constant_util

###########################################################

def get_soup(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def get_url(**kwargs):
    ti = kwargs['ti']
    select_track_list = ti.xcom_pull(task_ids='select_track_task')
    select_track_list = list(pd.DataFrame(select_track_list)['NAME'])[:10]
    #select_track_list = list(pd.DataFrame(context['task_instance'].xcom_pull(task_ids='select_track_task'))['NAME'])
    select_artist_list = ti.xcom_pull(task_ids='select_artist_task')
    select_artist_list = list(pd.DataFrame(select_artist_list)['NAME'])[:10]
    #select_artist_list = list(pd.DataFrame(context['task_instance'].xcom_pull(task_ids='select_artist_task'))['NAME'])

    artist_name_list = list(map(lambda x: quote(x), select_artist_list))
    track_name_list = list(map(lambda x: quote(x), select_track_list))

    basic_url = 'https://www.last.fm/music/'
    
    urls = []
    for track_name, artist_name in zip(track_name_list,artist_name_list):
        url = basic_url + artist_name.replace(' ', '+') + '/_/' + track_name.replace(' ', '+')
        urls.append(url)

    logging.info("spotify_track_id_list length: " + str(len(select_track_list)))
    logging.info("spotify_track_id_list length: " + str(len(select_artist_list)))

    return urls

def get_spotify_id(**kwargs):
    ti = kwargs['ti']
    select_spotify_track_id_list = list(pd.DataFrame(ti.xcom_pull(task_ids='select_spotify_track_id_task'))['SPOTIFY_TRACK_ID'])[:10]
    logging.info("Getting spotify id successfully")
    return select_spotify_track_id_list

def get_listeners(soup):
    try:
        listeners = soup.select('div.header-new-info-desktop>ul.header-metadata-tnew>li.header-metadata-tnew-item>div.header-metadata-tnew-display')[0].text.strip()
        if listeners[-1] == 'K':
            listeners = float(listeners[:-1]) * 1000
        elif listeners[-1] == 'M':
            listeners = float(listeners[:-1]) * 1000000
        else:
            listeners = float(listeners.replace(',',''))
    except:
        listeners = '0'
    return int(listeners)

def get_length(soup):
    try:
        length = soup.select('div.container.page-content>div.row')[0].find('div', class_='col-main buffer-standard buffer-reset@sm').select('div.metadata-column>dl.catalogue-metadata>dd.catalogue-metadata-description')[0].text.strip()
        length = int(length[0]) * 60 + int(length[2:])
    except:
        length = ''
    return length

def get_genres(soup):
    try:
        group = soup.select('div.container.page-content>div.row')[0].find('div', class_='row buffer-3 buffer-4@sm').select('div.col-sm-8>div.section-with-separator.section-with-separator--xs-only>section.catalogue-tags>ul.tags-list.tags-list--global')[0].find_all('li', class_='tag')
        genres = [genre.text for genre in group]
    except:
        genres = ''
    return genres

def get_introduction(soup):
    try:
        introduction = soup.find_all('div',class_='wiki-content')[0].text.strip()
    except:
        introduction = ''
    return introduction

def get_reviews(review_url,soup):
    review_collection = []
    # 리뷰 페이지 여러개 일 때
    try:
        pages = int(soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>nav.pagination>ul.pagination-list')[0].find_all('li', class_='pagination-page')[-1].text.strip())
        for page in range(1,pages+1):
            x = review_url + str(page)
            html = requests.get(x).text
            soup = BeautifulSoup(html, 'html.parser')
            reviews = soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>ul.shout-list.js-shout-list')[0]
            for review in reviews.select('li.shout-list-item.js-shout-list-item.js-shouts-container>div.shout-container>div.shout.js-shout.js-link-block'):
                dic = {}
                try:
                    dic['review'] = review.find('div',class_='shout-body').text.strip().replace('\r', ' ').replace('\n', ' ')
                except:
                    dic['review'] = ''
                try:
                    date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                    datetime_obj = datetime.strptime(date_str, '%d %b %Y, %I:%M%p')
                    dic['date'] = datetime_obj.strftime('%Y-%m-%d %H:%M')
                except:
                    try:    
                        date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                        datetime_obj = datetime.strptime(date_str, '%d %b %I:%M%p')
                        dic['date'] = str(datetime.now().year)+ '-' +datetime_obj.strftime('%m-%d %H:%M')
                    except:
                        dic['date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
                try:    
                    dic['likes'] = int(review.select('ul.shout-actions>li.shout-action>div.vote-button-toggle')[0].text.strip().split('\n')[-1].strip()[:-6])
                except:
                    dic['likes'] = 0
                review_collection.append(dic)

    except:
        # 리뷰 없음
        try:
            test = soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>p.no-data-message.js-shouts-insertion-hook')[0].text.strip()
            if test[:6] == 'Nobody':
                review_collection = ''
        # 리뷰 페이지 한 개
        except:
            reviews = soup.select('div.col-main>section>section.js-shouts-container.shoutbox.shoutbox--subpage>ul.shout-list.js-shout-list')[0]
            for review in reviews.select('li.shout-list-item.js-shout-list-item.js-shouts-container>div.shout-container>div.shout.js-shout.js-link-block'):
                dic = {}
                try:
                    dic['review'] = review.find('div',class_='shout-body').text.strip().replace('\r', ' ').replace('\n', ' ')
                except:
                    dic['review'] = ''
                try:
                    date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                    datetime_obj = datetime.strptime(date_str, '%d %b %Y, %I:%M%p')
                    dic['date'] = datetime_obj.strftime('%Y-%m-%d %H:%M')
                except:
                    try:    
                        date_str = review.find('a',class_='shout-permalink shout-timestamp').text.strip()
                        datetime_obj = datetime.strptime(date_str, '%d %b %I:%M%p')
                        dic['date'] = str(datetime.now().year)+ '-' +datetime_obj.strftime('%m-%d %H:%M')
                    except:
                        dic['date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
                try:
                    dic['likes'] = int(review.select('ul.shout-actions>li.shout-action>div.vote-button-toggle')[0].text.strip().split('\n')[-1].strip()[:-6])
                except:
                    dic['likes'] = 0
                review_collection.append(dic)
    return review_collection

###########################################################
# urls => 새로 추가되는 곡만

def get_info(**kwargs):
    urls = get_url(**kwargs)
    spotify_ids = get_spotify_id(**kwargs)
    info_dic = {}
    review_dic = {}
    for i in range(len(urls)):
        wiki_url = urls[i] + '/+wiki'
        review_url = urls[i] + '/+shoutbox/?page='
        info_value_dic = {}
        review_value_dic = {}
        
        try:
            soup = get_soup(urls[i])
            info_value_dic['listeners'] = get_listeners(soup)
            info_value_dic['length'] = get_length(soup)
            info_value_dic['genres'] = get_genres(soup)
            info_value_dic['last_fm_url'] = urls[i]

            soup = get_soup(wiki_url)
            info_value_dic['introduction'] = get_introduction(soup)

            soup = get_soup(review_url)
            review_value_dic['reviews'] = get_reviews(review_url,soup)

            info_dic[spotify_ids[i]] = info_value_dic
            review_dic[spotify_ids[i]] = review_value_dic
        except:
            continue
    logging.info('Getting info successfully!')
    return info_dic, review_dic

##############################################################

def get_last_fm_to_json(**kwargs):
    info_dic,review_dic = get_info(**kwargs)

    reviews_src_path = os.path.join(constant_util.DOWNLOADS_DIR, f'last_fm/reviews')
    info_src_path = os.path.join(constant_util.DOWNLOADS_DIR, f'last_fm/information')
    os.makedirs(reviews_src_path, exist_ok=True)
    os.makedirs(info_src_path, exist_ok=True)
    logging.info("Getting info start!")
    for spotify_id in list(info_dic.keys()):
        review_dic_json = {}
        info_dic_json = {}

        reviews_json_path = os.path.join(reviews_src_path, f'{spotify_id}.json')
        review_dic_json[spotify_id] = review_dic[spotify_id]

        with open(reviews_json_path, 'w', encoding='utf-8') as review_json_file:
            json.dump(review_dic_json, review_json_file, ensure_ascii=False, indent=4)

        info_json_path = os.path.join(info_src_path, f'{spotify_id}.json')
        info_dic_json[spotify_id] = info_dic[spotify_id]
        with open(info_json_path, 'w', encoding='utf-8') as info_json_file:
            json.dump(info_dic_json, info_json_file, ensure_ascii=False, indent=4)
        logging.info(f'Getting {spotify_id} info successfully!')
######################################################################################

def upload_raw_files_to_s3(bucket_name: str) -> None:
    reviews_src_path = os.path.join(constant_util.DOWNLOADS_DIR, f'last_fm/reviews')
    reviews_path = os.path.join(reviews_src_path, '*.json')

    info_src_path = os.path.join(constant_util.DOWNLOADS_DIR, f'last_fm/information')
    info_path = os.path.join(info_src_path, '*.json')

    src_paths = [reviews_path, info_path]

    for src_path in src_paths:
        filenames = glob.glob(src_path)
        keys = [filename.replace(constant_util.AIRFLOW_HOME, "")[1:] for filename in filenames]
        common_util.upload_files_to_s3(filenames=filenames, keys=keys, bucket_name=bucket_name, replace=True)

with DAG(dag_id="last_fm_dag",
         schedule_interval="@daily",
         start_date=datetime(2024, 1, 1),
         catchup=False) :
    
    start_task = EmptyOperator(
        task_id="start_task"
    )

    select_track_task = SnowflakeOperator(
        task_id="select_track_task",
        sql= url.select_track,
        snowflake_conn_id='s3_to_snowflake',
        do_xcom_push=True
    )

    select_artist_task = SnowflakeOperator(
        task_id="select_artist_task",
        sql= url.select_artist,
        snowflake_conn_id='s3_to_snowflake',
        do_xcom_push=True
    )

    select_spotify_track_id_task = SnowflakeOperator(
        task_id="select_spotify_track_id_task",
        sql= url.select_spotify_track_id,
        snowflake_conn_id='s3_to_snowflake',
        do_xcom_push=True
    )


    last_fm_to_json_task = PythonOperator(
        task_id = "last_fm_to_json_task",
        python_callable=get_last_fm_to_json,
        provide_context = True
    )

    upload_raw_files_to_s3_task = PythonOperator(
        task_id = "upload_raw_files_to_s3_task",
        python_callable= upload_raw_files_to_s3,
        op_kwargs= {
            "bucket_name": constant_util.BUCKET_NAME
        }
    )

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='transform_dag',
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

    start_task >>[select_track_task, select_artist_task, select_spotify_track_id_task] >> last_fm_to_json_task >> call_trigger_task >> upload_raw_files_to_s3_task >> end_task