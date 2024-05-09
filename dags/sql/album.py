def select_album(bucket_name: str, date: str=None):
    sql = f"""
    USE SCHEMA PUBLIC;

    CREATE TABLE IF NOT EXISTS album(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_album_id varchar NULL,
        name varchar NULL,
        total_tracks int NULL,
        album_type varchar NULL,
        release_date date NULL,
        release_date_precision varchar NULL
    );

    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;


    CREATE OR REPLACE STAGE album_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/albums/{date}/'
        FILE_FORMAT = my_csv_format;


    list @album_stage;   

    COPY INTO SPOTIFY.PUBLIC.ALBUM (spotify_album_id, name, total_tracks, album_type, release_date, release_date_precision) 
    FROM (
        SELECT $1spotify_album_id, $2name, $3total_tracks, $4album_type, $5release_date, $6release_date_precision
        FROM '@album_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'ABORT_STATEMENT'; 

    COMMIT
    """

    return sql