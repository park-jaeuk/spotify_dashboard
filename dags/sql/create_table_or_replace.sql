CREATE OR REPLACE TABLE artist (
    id bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	varchar	NULL,
	name	varchar	NULL,
	type	varchar	NULL,
	follwers	int	NULL
);

CREATE OR REPLACE TABLE album (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	spotify_id	varchar	NULL,
	name	varchar	NULL,
	total_tracks	int	NULL,
	album_type	varchar	NULL,
	release_date	datetime	NULL,
	release_date_precision	datetime	NULL
);

CREATE OR REPLACE TABLE track (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	album_id	bigint	NOT NULL,
	spotify_id	varchar	NULL,
	name	varchar	NULL,
	duration_ms	bigint	NULL
);

CREATE OR REPLACE TABLE genre (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,,
	name	varchar	NULL
);

CREATE OR REPLACE TABLE track_artist (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	artist_id	bigint	NOT NULL,
	track_id	bigint	NOT NULL,
	type	varchar	NULL
);


CREATE OR REPLACE TABLE album_genre (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	album_id	bigint	NOT NULL,
	genre_id	bigint	NOT NULL
);

CREATE OR REPLACE TABLE track_chart (
	id	bigint	NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
	track_id	bigint	NULL,
	now_rank	int	NULL,
	peak_rank	int	NULL,
	previous_rank	int	NULL,
	total_days_on_chart	int	NULL,
	stream_count	bigint	NULL,
    region      varchar(50)  NULL,
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

ALTER TABLE track_info ADD CONSTRAINT PK_TRACK_INFO PRIMARY KEY (
	id
);

ALTER TABLE track ADD CONSTRAINT FK_album_TO_track_1 FOREIGN KEY (
	album_id
)
REFERENCES album (
	id
);

ALTER TABLE track_artist ADD CONSTRAINT FK_artist_TO_track_artist_1 FOREIGN KEY (
	artist_id
)
REFERENCES artist (
	id
);

ALTER TABLE track_artist ADD CONSTRAINT FK_track_TO_track_artist_1 FOREIGN KEY (
	track_id
)
REFERENCES track (
	id
);

ALTER TABLE album_genre ADD CONSTRAINT FK_album_TO_album_genre_1 FOREIGN KEY (
	album_id
)
REFERENCES album (
	id
);

ALTER TABLE album_genre ADD CONSTRAINT FK_genre_TO_album_genre_1 FOREIGN KEY (
	genre_id
)
REFERENCES genre (
	id
);

ALTER TABLE track_chart ADD CONSTRAINT FK_track_TO_track_chart_1 FOREIGN KEY (
	track_id
)
REFERENCES track (
	id
);

