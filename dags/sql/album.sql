USE WAREHOUSE SPOTIFY_WH;

CREATE or replace STAGE album_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://airflow-gin-bucket/transform/spotify/api/albums/2024-03-11/';


CREATE OR REPLACE TABLE album (
	id	bigint	NOT NULL,
	spotify_id	varchar	NULL,
	name	varchar	NULL,
	total_tracks	int	NULL,
	album_type	varchar	NULL,
	release_date	datetime	NULL,
	release_date_precision	varchar	NULL
);

COPY INTO album (spotify_id, name, total_tracks, album_type, release_date, release_date_precision)
FROM (
    SELECT $1spotify_id, $2name, $3total_tracks, $3album_type,
        $5release_date, $6release_date_precision
    FROM '@album_stage/transfrom_album.csv'
)
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
ON_ERROR = 'ABORT_STATEMENT'; 
        
COMMIT