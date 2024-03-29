from airflow.models.variable import Variable

from datetime import datetime, timedelta
import os
import pendulum

SPOTIFY_CLIENT_ID = Variable.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET")
BUCKET_NAME = Variable.get("BUCKET_NAME")
AIRFLOW_HOME = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
DOWNLOADS_DIR = os.path.join(AIRFLOW_HOME, 'downloads')
TRANSFORM_DIR = os.path.join(AIRFLOW_HOME, 'transform')

# # timezone 설정
# local_tz = pendulum.timezone("Asia/Seoul")
# # 현재 시간 설정
# NOW_DATE = datetime.now(tz=local_tz).strftime('%Y-%m-%d')
US_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
# US_DATE = "2024-03-22"
NOW_DATE = "2024-03-11"