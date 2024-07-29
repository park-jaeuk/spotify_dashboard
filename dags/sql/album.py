def select_album(bucket_name: str):
    sql = f"""
    USE SCHEMA PUBLIC;

    drop table album;

    -- 최종 테이블 생성
    CREATE TABLE IF NOT EXISTS album(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_album_id varchar NOT NULL,
        name varchar NOT  NULL,
        total_tracks int NOT NULL,
        album_type varchar NOT NULL,
        release_date date NOT NULL,
        release_date_precision varchar NOT NULL,
        PRIMARY KEY (spotify_album_id)
    );

    -- 파일 포맷 생성
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    -- 스테이지 생성
    CREATE OR REPLACE STAGE album_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/albums'
        FILE_FORMAT = my_csv_format;

    -- 데이터 확인
    LIST @album_stage;

    -- 임시 테이블 생성
    CREATE OR REPLACE TEMPORARY TABLE temp_album(
        spotify_album_id varchar NOT NULL,
        name varchar NOT  NULL,
        total_tracks int NOT NULL,
        album_type varchar NOT NULL,
        release_date date NOT NULL,
        release_date_precision varchar NOT NULL
    );

    -- 각 스테이지로부터 임시 테이블에 데이터 로드
    COPY INTO temp_album (spotify_album_id, name, total_tracks, album_type, release_date, release_date_precision) 
    FROM (
        SELECT $1, $2, $3, $4, $5, $6
        FROM '@album_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'CONTINUE'; 

    -- 중복 제거 후 최종 테이블로 데이터 이동
    INSERT INTO album (spotify_album_id, name, total_tracks, album_type, release_date, release_date_precision)
    SELECT DISTINCT spotify_album_id, name, total_tracks, album_type, release_date, release_date_precision
    FROM temp_album;

    -- 임시테이블 삭제
    drop table temp_album;

    -- 트랜잭션 커밋
    COMMIT;
    """

    return sql