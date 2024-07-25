def select_track(bucket_name: str) :
    sql = f"""
    USE SCHEMA PUBLIC;

    drop table track;

    -- 최종 테이블 생성
    CREATE TABLE IF NOT EXISTS track(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_track_id varchar NOT NULL,
        spotify_album_id varchar NOT NULL,
        name varchar NOT NULL,
        duration_ms bigint NOT NULL,
        PRIMARY KEY (spotify_track_id)
    );

    -- 파일 포맷 생성
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    -- 스테이지 생성
    CREATE OR REPLACE STAGE track_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/tracks/'
        FILE_FORMAT = my_csv_format;

    -- 데이터 확인
    LIST @track_stage;

    -- 임시 테이블 생성
    CREATE OR REPLACE TEMPORARY TABLE temp_track(
        spotify_track_id varchar NOT NULL,
        spotify_album_id varchar NOT NULL,
        name varchar NOT NULL,
        duration_ms bigint NOT NULL
    );

    -- 각 스테이지로부터 임시 테이블에 데이터 로드
    COPY INTO temp_track (spotify_track_id, spotify_album_id, name, duration_ms) 
    FROM (
        SELECT $1, $2, $3, $4
        FROM '@track_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'CONTINUE'; 

    -- 중복 제거 후 최종 테이블로 데이터 이동
    INSERT INTO track (spotify_track_id, spotify_album_id, name, duration_ms)
    SELECT DISTINCT spotify_track_id, spotify_album_id, name, duration_ms
    FROM temp_track;

    -- 트랜잭션 커밋
    COMMIT;
        """

    return sql