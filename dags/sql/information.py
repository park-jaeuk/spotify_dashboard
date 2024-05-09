select_information = """
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE information(
    id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    spotify_track_id varchar NULL,
    listeners bigint NULL,
    duration bigint NULL,
    wiki varchar NULL,
    last_fm_url varchar NULL
);


CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

CREATE OR REPLACE STAGE information_stage
    STORAGE_INTEGRATION = spotify_api_to_snowflake
    URL = 's3://demobyjay/transform/last_fm/information/'
    FILE_FORMAT = my_csv_format;
    

COPY INTO SPOTIFY.PUBLIC.INFORMATION (spotify_track_id, listeners, duration, wiki, last_fm_url) 
FROM (
    SELECT $1spotify_track_id, $2listeners, $3duration, $4wiki, $5last_fm_url
    FROM '@information_stage'
)
FILE_FORMAT = my_csv_format
ON_ERROR = 'ABORT_STATEMENT';


"""