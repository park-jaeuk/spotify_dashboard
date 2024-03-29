CREATE TABLE IF NOT EXISTS artist (
    id BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	VARCHAR	NULL,
	name	VARCHAR	NULL,
	type	VARCHAR	NULL
);

CREATE TABLE IF NOT EXISTS album (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	VARCHAR	NULL,
	name	VARCHAR	NULL,
	total_tracks	INT	NULL,
	album_type	VARCHAR	NULL,
	release_date	DATETIME	NULL,
	release_date_precision	VARCHAR	NULL
);

CREATE TABLE IF NOT EXISTS TABLE track (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	varchar	NULL,
    spotify_album_id	varchar NULL,
	name	varchar	NULL,
	duration_ms	bigint	NULL
);

CREATE TABLE IF NOT EXISTS genre (
	id	BIGINT	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	name	VARCHAR	NULL
);

CREATE TABLE IF NOT EXISTS track_artist (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_artist_id	varchar	NOT NULL,
	spotify_track_id	varchar	NOT NULL,
	type	varchar	NULL
);


CREATE TABLE IF NOT EXISTS album_genre (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	album_id	bigint	NOT NULL,
	genre_id	bigint	NOT NULL
);

CREATE TABLE IF NOT EXISTS track_chart (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_track_id	bigint	NULL,
	now_rank	int	NULL,
	peak_rank	int	NULL,
	previous_rank	int	NULL,
	total_days_on_chart	int	NULL,
	stream_count	bigint	NULL,
    region      varchar  NULL,
	chart_date	datetime	NULL
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