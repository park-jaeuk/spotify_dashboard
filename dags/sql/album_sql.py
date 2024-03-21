create_album_table = """
CREATE TABLE IF NOT EXISTS album (
	id	bigint	NOT NULL,
	spotify_id	bigint	NULL,
	name	varchar	NULL,
	total_tracks	int	NULL,
	album_type	varchar	NULL,
	release_date	datetime	NULL,
	release_date_precision	datetime	NULL
);
"""

commit = "COMMIT"

# COPY INTO의 경우 한 번만 실행시켜야 들어감 (아직 왜 2번 이상하면 안되는지 모르겠음)
# 따라서 현재 테스트 작업 중 일 때는 CREATE OR REPLACE를 실행 (기존 데이터가 사라짐)
# 추후에 CREATE TABLE IF NOT EXISTS로 변경 예정


sql = """
USE SCHEMA SPOTIFY_SCHEMA;

CREATE OR REPLACE STAGE album_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://airflow-gin-bucket/transform/spotify/api/albums/2024-03-11/';
    
CREATE OR REPLACE TABLE album (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	varchar	NULL,
	name	varchar	NULL,
	total_tracks	int	NULL,
	album_type	varchar	NULL,
	release_date	datetime	NULL,
	release_date_precision	varchar	NULL
);

COPY INTO album (spotify_id, name, total_tracks, album_type, release_date, release_date_precision)
FROM (
    SELECT $1spotify_id, $2name, $3total_tracks, $3album_type,
        $5release_date, $6release_date_precision
    FROM '@album_stage/transform_album.csv'
)
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
ON_ERROR = 'ABORT_STATEMENT'; 
        
COMMIT
"""