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