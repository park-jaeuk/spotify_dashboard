CREATE OR REPLACE TABLE artist (
    id BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	VARCHAR	NULL,
	name	VARCHAR	NULL,
	type	VARCHAR	NULL
);

CREATE OR REPLACE TABLE album (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	VARCHAR	NULL,
	name	VARCHAR	NULL,
	total_tracks	INT	NULL,
	album_type	VARCHAR	NULL,
	release_date	DATETIME	NULL,
	release_date_precision	VARCHAR	NULL
);

CREATE OR REPLACE TABLE track (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	VARCHAR	NULL,
    spotify_album_id	VARCHAR NULL,
	name	VARCHAR	NULL,
	duration_ms	BIGINT	NULL
);

CREATE OR REPLACE TABLE genre (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	name	VARCHAR	NULL
);

CREATE OR REPLACE TABLE track_artist (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_artist_id	VARCHAR	NOT NULL,
	spotify_track_id	VARCHAR	NOT NULL,
	type	VARCHAR	NULL
);


CREATE OR REPLACE TABLE album_genre (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	album_id	VARCHAR	NOT NULL,
	genre_id	VARCHAR	NOT NULL
);

CREATE OR REPLACE TABLE track_chart (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_track_id	BIGINT	NULL,
	now_rank	INT	NULL,
	peak_rank	INT	NULL,
	previous_rank	INT	NULL,
	total_days_on_chart	INT	NULL,
	stream_count	BIGINT	NULL,
    region      VARCHAR  NULL,
	chart_date	DATETIME	NULL
);


ALTER TABLE artist ADD CONSTRAINT PK_ARTIST PRIMARY KEY (
	id
);

ALTER TABLE album ADD CONSTRAINT PK_ALBUM PRIMARY KEY (
	id
);

ALTER TABLE track ADD CONSTRAINT PK_TRACK PRIMARY KEY (
	id
);

ALTER TABLE genre ADD CONSTRAINT PK_GENRE PRIMARY KEY (
	id
);

ALTER TABLE track_artist ADD CONSTRAINT PK_TRACK_ARTIST PRIMARY KEY (
	id
);

ALTER TABLE album_genre ADD CONSTRAINT PK_ALBUM_GENRE PRIMARY KEY (
	id
);

ALTER TABLE track_chart ADD CONSTRAINT PK_TRACK_CHART PRIMARY KEY (
	id
);
