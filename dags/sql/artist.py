select_artist = """
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE artist(
    id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    spotify_artist_id varchar NULL,
    name varchar NULL,
    type varchar NULL
);


CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;


CREATE OR REPLACE STAGE artist_stage
    STORAGE_INTEGRATION = spotify_api_to_snowflake
    URL = 's3://demobyjay/transform/spotify/api/artists/'
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