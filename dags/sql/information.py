def select_information (bucket_name: str, date: str=None) :
    sql = f"""
    USE SCHEMA PUBLIC;

    drop table information;

    -- 최종 테이블 생성
    CREATE TABLE IF NOT EXISTS information(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_track_id varchar NOT NULL,
        listeners bigint NULL,
        length bigint NULL,
        last_fm_url varchar NOT NULL,
        introduction varchar NULL,
        PRIMARY KEY (spotify_track_id, last_fm_url)
    );

    -- 파일 포맷 생성
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    -- 스테이지 생성
    CREATE OR REPLACE STAGE information_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/last_fm/information/'
        FILE_FORMAT = my_csv_format;

    -- 데이터 확인
    LIST @information_stage;

    -- 임시 테이블 생성
    CREATE OR REPLACE TEMPORARY TABLE temp_information(
        spotify_track_id varchar NOT NULL,
        listeners bigint NULL,
        length bigint NULL,
        last_fm_url varchar NOT NULL,
        introduction varchar NULL
    );

    -- 각 스테이지로부터 임시 테이블에 데이터 로드
    COPY INTO temp_information (spotify_track_id, listeners, length, last_fm_url, introduction) 
    FROM (
        SELECT $1, $2, $3, $4, $5
        FROM '@information_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'CONTINUE'; 

    -- 중복 제거 후 최종 테이블로 데이터 이동
    INSERT INTO information (spotify_track_id, listeners, length, last_fm_url, introduction)
    SELECT DISTINCT spotify_track_id, listeners, length, last_fm_url, introduction
    FROM temp_information;

    -- 트랜잭션 커밋
    COMMIT;
    """

    return sql

