def select_track_chart(bucket_name: str, date: str=None) :
    sql = f"""
    USE SCHEMA PUBLIC;

    CREATE TABLE IF NOT EXISTS  track_chart(
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


    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;


    CREATE OR REPLACE STAGE track_chart_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/track-charts/{date}/'
        FILE_FORMAT = my_csv_format;

    COPY INTO SPOTIFY.PUBLIC.TRACK_CHART (spotify_track_id, now_rank, peak_rank, previous_rank, total_days_on_chart, stream_count, region, chart_date) 
    FROM (
        SELECT $1spotify_track_id, $2now_rank, $3peak_rank, $4previous_rank, $5total_days_on_chart, $6region, $7stream_count,               $8chart_date
        FROM '@track_chart_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'ABORT_STATEMENT'; 
    """

    return sql