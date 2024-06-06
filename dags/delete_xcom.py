from airflow.models import XCom
from airflow.utils.session import create_session

# 설정할 DAG ID와 Task ID
dag_id = 'test_dag'
task_id = 'select_spotify_track_id_recent_review_date_task'

# 데이터베이스 세션 생성 및 XCom 데이터 삭제
with create_session() as session:
    session.query(XCom).filter(
        XCom.dag_id == dag_id,
        XCom.task_id == task_id
    ).delete(synchronize_session=False)
    session.commit()
