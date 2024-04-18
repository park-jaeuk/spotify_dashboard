USE SCHEMA SPOTIFY_SCHEMA;

CREATE OR REPLACE STAGE track_artist_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://airflow-gin-bucket/transform/spotify/track_artists/2024-03-11/';

CREATE OR REPLACE TABLE track_artist (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_artist_id	VARCHAR	NOT NULL,
	spotify_track_id	VARCHAR	NOT NULL,
	type	VARCHAR	NULL
);


COPY INTO track_artist (spotify_artist_id, spotify_track_id)
FROM (
SELECT $1spotify_track_id, $2spotify_track_id
  FROM '@track_artist_stage/transform_track.csv'
)
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1,
    FIELD_OPTIONALLY_ENCLOSED_BY='"', ESCAPE_UNENCLOSED_FIELD = NONE)
ON_ERROR = 'ABORT_STATEMENT'; 
        
COMMIT