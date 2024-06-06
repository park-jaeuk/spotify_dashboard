def select_track_artist(bucket_name: str, date: str=None) :
    sql = f"""
    USE SCHEMA PUBLIC;

    CREATE TABLE IF NOT EXISTS track_artist(
        spotify_track_id varchar NOT NULL,
        spotify_artist_id varchar NOT NULL
    );


    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;


    CREATE OR REPLACE STAGE track_artist_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/track-artists/{date}/'
        FILE_FORMAT = my_csv_format;


    COPY INTO SPOTIFY.PUBLIC.TRACK_ARTIST (spotify_track_id, spotify_artist_id) 
    FROM (
        SELECT $1spotify_track_id, $2spotify_artist_id
        FROM '@track_artist_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'ABORT_STATEMENT';
    """

    return sql