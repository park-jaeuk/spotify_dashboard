USE SCHEMA SPOTIFY_SCHEMA;

CREATE OR REPLACE STAGE track_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://airflow-gin-bucket/transform/spotify/api/tracks/2024-03-11/';

CREATE OR REPLACE TABLE track (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	varchar	NULL,
  spotify_album_id	varchar NULL,
	name	varchar	NULL,
	duration_ms	bigint	NULL
);


COPY INTO track_id (spotify_album_id, spotify_id, name, duration_ms)
FROM (
    SELECT $1spotify_album_id, $2spotify_id, $3name, $3duration_ms
    FROM '@track_stage/transform_track.csv'
)
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
ON_ERROR = 'ABORT_STATEMENT'; 
        
COMMIT