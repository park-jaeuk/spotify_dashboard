select_review = """
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE reviews(
    id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
    spotify_track_id varchar NULL,
    reviews_date datetime NULL,
    contents varchar NULL,
    likes bigint NULL
);


CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;


CREATE OR REPLACE STAGE reviews_stage
    STORAGE_INTEGRATION = spotify_api_to_snowflake
    URL = 's3://demobyjay/transform/last_fm/reviews/'
    FILE_FORMAT = my_csv_format;
    
    
COPY INTO SPOTIFY.PUBLIC.REVIEWS (spotify_track_id, contents, reviews_date, likes) 
FROM (
    SELECT $1spotify_track_id, $2contents, $3reviews_date, $4likes
    FROM '@reviews_stage'
)
FILE_FORMAT = my_csv_format
ON_ERROR = 'ABORT_STATEMENT';  
 
COMMIT
    """