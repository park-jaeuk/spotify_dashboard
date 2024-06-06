from airflow.models.variable import Variable
from datetime import datetime, timedelta
import logging
import os

class Config:
    SPOTIFY = Variable.get("SPOTIFY", deserialize_json=True)
    SPOTIFY_CLIENT_IDS = SPOTIFY["CLIENT_IDS"]
    SPOTIFY_CLIENT_SECRETS = SPOTIFY["CLIENT_SECRETS"]

    SPOTIFY_CLIENT_ID = SPOTIFY["CLIENT_IDS"][0]
    SPOTIFY_CLIENT_SECRET = SPOTIFY["CLIENT_SECRETS"][0]

    SPOTIFY_CHARTS_LOGIN_USERNAME = Variable.get("SPOTIFY_CHARTS_LOGIN_USERNAME")
    SPOTIFY_CHARTS_LOGIN_PASSWORD = Variable.get("SPOTIFY_CHARTS_LOGIN_PASSWORD")
    BUCKET_NAME = Variable.get("BUCKET_NAME")

    REGIONS = Variable.get("REGIONS", deserialize_json=True)

class Directory:
    AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
    DOWNLOADS_DIR = os.path.join(AIRFLOW_HOME, 'downloads')
    TRANSFORM_DIR = os.path.join(AIRFLOW_HOME, 'transform')

# # timezone 설정
# local_tz = pendulum.timezone("Asia/Seoul")
# # 현재 시간 설정
# NOW_DATE = datetime.now(tz=local_tz).strftime('%Y-%m-%d')
#US_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
# US_DATE = "2024-03-22"

class Date:    
    US_DATE = Variable.get("US_DATE")

    @classmethod
    def plus_one_day(cls):
        date_obj = datetime.strptime(cls.US_DATE, "%Y-%m-%d")
        next_day = (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')
        Variable.set("US_DATE", next_day)
        logging.info(Variable.get("US_DATE"))