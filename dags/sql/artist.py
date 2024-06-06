def select_artist(bucket_name: str, date: str=None):
    sql = f"""
    USE SCHEMA PUBLIC;

    CREATE TABLE IF NOT EXISTS artist(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_artist_id varchar NOT NULL,
        name varchar NOT NULL,
        type varchar NOT NULL
    );


    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;


    CREATE OR REPLACE STAGE artist_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/spotify/artists/{date}/'
        FILE_FORMAT = my_csv_format;


    COPY INTO SPOTIFY.PUBLIC.ARTIST (spotify_artist_id, name, type) 
    FROM (
        SELECT $1spotify_id, $2name, $3type
        FROM '@artist_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'ABORT_STATEMENT'; 

    COMMIT
    """

    return sql