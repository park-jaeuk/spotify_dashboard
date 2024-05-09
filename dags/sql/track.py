def select_track(bucket_name: str, date: str=None) :
    sql = f"""
    USE SCHEMA PUBLIC;

    CREATE TABLE IF NOT EXISTS  track(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_track_id varchar NULL,
        spotify_album_id varchar NULL,
        name varchar NULL,
        duration_ms bigint NULL
    );


    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;

    CREATE OR REPLACE STAGE track_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/tracks/{date}/'
        FILE_FORMAT = my_csv_format;

    COPY INTO SPOTIFY.PUBLIC.TRACK (spotify_track_id, spotify_album_id, name, duration_ms) 
    FROM (
        SELECT $1spotify_track_id, $2spotify_album_id, $3name, $4duration_ms
        FROM '@track_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'ABORT_STATEMENT'; 
    """

    return sql