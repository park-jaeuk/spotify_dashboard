from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from utils.constant_util import Date

with DAG(dag_id="setting_date_dag",
         schedule_interval=timedelta(minutes=10), # trigger DAG는 보통 None으로 처리 합니다.
         start_date=datetime(2024, 1, 1),
         catchup=False) :

    setting_date_task = PythonOperator(
        task_id = "transform_artist_csv_task",
        python_callable=Date.plus_one_day
    )

    call_trigger_task = TriggerDagRunOperator(
        task_id='call_trigger',
        trigger_dag_id='spotify_charts_dag',
        trigger_run_id=None,
        execution_date=None,
        reset_dag_run=False,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None,
    )

