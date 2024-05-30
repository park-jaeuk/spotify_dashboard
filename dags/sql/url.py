select_track = """
select DISTINCT track.name
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""

select_artist = """
select DISTINCT artist.name
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""

select_spotify_track_id = """
select DISTINCT track.spotify_track_id
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""

select_spotify_track_id_recent_review_date = """
SELECT SPOTIFY_TRACK_ID, TO_CHAR(MAX(REVIEWS_DATE), 'YYYY-MM-DD HH24:MI') AS MAX_REVIEWS_DATE
FROM reviews
GROUP BY SPOTIFY_TRACK_ID;
"""


select_spotify_track_id_track_artist = """
select distinct track_artist.spotify_track_id, track.name as track, artist.name as artist
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""

select_last_fm = """
SELECT 
    distinct ta.spotify_track_id, 
    t.name AS track, 
    a.name AS artist, 
    r.max_reviews_date
FROM 
    (SELECT 
        spotify_track_id, 
        TO_CHAR(MAX(reviews_date), 'YYYY-MM-DD HH24:MI') AS max_reviews_date
    FROM 
        reviews
    GROUP BY 
        spotify_track_id) r
JOIN 
    track_artist ta ON r.spotify_track_id = ta.spotify_track_id
JOIN 
    track t ON ta.spotify_track_id = t.spotify_track_id
JOIN 
    artist a ON ta.spotify_artist_id = a.spotify_artist_id;
"""