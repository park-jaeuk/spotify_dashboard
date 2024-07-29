def select_artist(bucket_name: str):
    sql = f"""
    USE SCHEMA PUBLIC;

    drop table artist;

    -- 최종 테이블 생성
    CREATE TABLE IF NOT EXISTS artist(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_artist_id varchar NOT NULL,
        name varchar NOT NULL,
        type varchar NOT NULL,
        PRIMARY KEY (spotify_artist_id)
    );

    -- 파일 포맷 생성
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    -- 스테이지 생성
    CREATE OR REPLACE STAGE artist_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/artists/'
        FILE_FORMAT = my_csv_format;

    -- 데이터 확인
    LIST @artist_stage;

    -- 임시 테이블 생성
    CREATE OR REPLACE TEMPORARY TABLE temp_artist(
        spotify_artist_id varchar NOT NULL,
        name varchar NOT NULL,
        type varchar NOT NULL
    );

    -- 각 스테이지로부터 임시 테이블에 데이터 로드
    COPY INTO temp_artist (spotify_artist_id, name, type) 
    FROM (
        SELECT $1, $2, $3
        FROM '@artist_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'CONTINUE'; 

    -- 중복 제거 후 최종 테이블로 데이터 이동
    INSERT INTO artist (spotify_artist_id, name, type)
    SELECT DISTINCT spotify_artist_id, name, type
    FROM temp_artist;

    -- 임시테이블 삭제
    drop table temp_artist;

    -- 트랜잭션 커밋
    COMMIT;
    """

    return sql