def select_tag(bucket_name: str, date: str=None) :
    sql  = f"""
    USE SCHEMA PUBLIC;

    drop table tags;

    -- 최종 테이블 생성
    CREATE TABLE IF NOT EXISTS tags(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_track_id varchar NOT NULL,
        tags varchar NOT NULL,
        PRIMARY KEY (spotify_track_id, tags)
    );

    -- 파일 포맷 생성
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    -- 스테이지 생성
    CREATE OR REPLACE STAGE tags_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/last_fm/tags/'
        FILE_FORMAT = my_csv_format;

    -- 데이터 확인
    LIST @tags_stage;

    -- 임시 테이블 생성
    CREATE OR REPLACE TEMPORARY TABLE temp_tags(
        spotify_track_id varchar NOT NULL,
        tags varchar NOT NULL
    );

    -- 각 스테이지로부터 임시 테이블에 데이터 로드
    COPY INTO temp_tags (spotify_track_id, tags) 
    FROM (
        SELECT $1, $2
        FROM '@tags_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'CONTINUE'; 

    -- 중복 제거 후 최종 테이블로 데이터 이동
    INSERT INTO tags (spotify_track_id, tags)
    SELECT DISTINCT spotify_track_id, tags
    FROM temp_tags;

    -- 트랜잭션 커밋
    COMMIT;

    """

    return sql