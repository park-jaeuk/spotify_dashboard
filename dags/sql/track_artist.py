select_track_artist = """
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE track_artist(
    spotify_track_id varchar NULL,
    spotify_artist_id varchar NULL
);


CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;


CREATE OR REPLACE STAGE track_artist_stage
    STORAGE_INTEGRATION = spotify_api_to_snowflake
    URL = 's3://demobyjay/transform/spotify/api/track_artist/'
    FILE_FORMAT = my_csv_format;


COPY INTO SPOTIFY.PUBLIC.TRACK_ARTIST (spotify_track_id, spotify_artist_id) 
FROM (
    SELECT $1spotify_track_id, $2spotify_artist_id
    FROM '@track_artist_stage'
)
FILE_FORMAT = my_csv_format
ON_ERROR = 'ABORT_STATEMENT';

"""