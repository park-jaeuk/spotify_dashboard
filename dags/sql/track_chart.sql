USE SCHEMA SPOTIFY_SCHEMA;

CREATE OR REPLACE STAGE track_chart_stage
    STORAGE_INTEGRATION = s3_int
    URL = 's3://airflow-gin-bucket/transform/spotify/charts/2024-03-11/';

CREATE OR REPLACE TABLE track_chart (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_track_id	varchar	NULL,
	now_rank	int	NULL,
	peak_rank	int	NULL,
	previous_rank	int	NULL,
	total_days_on_chart	int	NULL,
	stream_count	bigint	NULL,
    region      varchar  NULL,
	chart_date	datetime	NULL
);

COPY INTO track_chart (spotify_track_id, now_rank, peak_rank, previous_rank,
        total_days_on_chart, stream_count, region, chart_date)
FROM (
SELECT $1spotify_track_id, $2now_rank, $3peak_rank, $4previous_rank, 
    $5total_days_on_chart, $6stream_count, $7region, $8chart_date
  FROM '@track_chart_stage/transform-concat-daily-2024-03-11.csv'
)
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1,
    FIELD_OPTIONALLY_ENCLOSED_BY='"', ESCAPE_UNENCLOSED_FIELD = NONE)
ON_ERROR = 'ABORT_STATEMENT'; 
        
COMMIT