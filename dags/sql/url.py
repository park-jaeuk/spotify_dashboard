select_track = """
select track.name
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""

select_artist = """
select artist.name
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""

select_spotify_track_id = """
select track.spotify_track_id
from track_artist, track, artist
where track_artist.spotify_track_id = track.spotify_track_id and track_artist.spotify_artist_id = artist.spotify_artist_id;
"""