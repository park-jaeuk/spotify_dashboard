USE SCHEMA SPOTIFY_SCHEMA;

CREATE OR REPLACE STAGE artist_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://airflow-gin-bucket/transform/spotify/api/artists/2024-03-11/';

CREATE OR REPLACE TABLE artist (
    id bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	varchar	NULL,
	name	varchar	NULL,
	type	varchar	NULL
);

COPY INTO artist (spotify_id, name, type) 
FROM (
    SELECT $1spotify_id, $2name, $3type
    FROM '@artist_stage/transform_artist.csv'
)
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
ON_ERROR = 'ABORT_STATEMENT'; 
        
COMMIT