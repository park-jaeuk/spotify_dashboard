def select_track_chart(bucket_name: str) :
    sql = f"""
    USE SCHEMA PUBLIC;

    drop table track_chart;

    -- 최종 테이블 생성
    CREATE TABLE IF NOT EXISTS track_chart(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_track_id varchar NOT NULL,
        now_rank int NOT NULL,
        peak_rank int NOT NULL,
        previous_rank int NOT NULL,
        total_days_on_chart int NOT NULL,
        stream_count bigint NOT NULL,
        region varchar NOT NULL,
        chart_date date NOT NULL
    );

    -- 파일 포맷 생성
    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    -- 스테이지 생성
    CREATE OR REPLACE STAGE track_chart_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/track-charts/'
        FILE_FORMAT = my_csv_format;

    -- 데이터 확인
    LIST @track_chart_stage;

    -- 임시 테이블 생성
    CREATE OR REPLACE TEMPORARY TABLE temp_track_chart(
        spotify_track_id varchar NOT NULL,
        now_rank int NOT NULL,
        peak_rank int NOT NULL,
        previous_rank int NOT NULL,
        total_days_on_chart int NOT NULL,
        stream_count bigint NOT NULL,
        region varchar NOT NULL,
        chart_date date NOT NULL
    );

    -- 각 스테이지로부터 임시 테이블에 데이터 로드
    COPY INTO temp_track_chart (spotify_track_id, now_rank, peak_rank, previous_rank, total_days_on_chart, stream_count, region, chart_date) 
    FROM (
        SELECT $1, $2, $3, $4, $5, $6, $7, $8
        FROM '@track_chart_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'CONTINUE'; 

    -- 중복 제거 후 최종 테이블로 데이터 이동
    INSERT INTO track_chart (spotify_track_id, now_rank, peak_rank, previous_rank, total_days_on_chart, stream_count, region, chart_date)
    SELECT DISTINCT spotify_track_id, now_rank, peak_rank, previous_rank, total_days_on_chart, stream_count, region, chart_date
    FROM temp_track_chart;

    -- 임시테이블 삭제
    drop table temp_track_chart;


    -- 트랜잭션 커밋
    COMMIT;
        """

    return sql