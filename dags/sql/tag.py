select_tag = """
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE tags(
    id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    spotify_track_id varchar NULL,
    tags varchar NULL
);

CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;


CREATE OR REPLACE STAGE tags_stage
    STORAGE_INTEGRATION = spotify_api_to_snowflake
    URL = 's3://demobyjay/transform/last_fm/tags/'
    FILE_FORMAT = my_csv_format;

    

COPY INTO SPOTIFY.PUBLIC.TAGS (spotify_track_id, tags) 
FROM (
    SELECT $1spotify_track_id, $2tags
    FROM '@tags_stage'
)
FILE_FORMAT = my_csv_format
ON_ERROR = 'ABORT_STATEMENT';

"""