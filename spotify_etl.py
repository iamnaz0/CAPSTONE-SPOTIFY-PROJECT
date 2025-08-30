import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
from s3_uploader import upload_file_to_s3

from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
import requests

sp = Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    ),
    requests_timeout=30  # <-- this increases the timeout from 5s to 30s
)



def get_top_artists(limit=100):
    artists = []
    offset = 0

    while len(artists) < limit:
        results = sp.search(q='year:2023', type='artist', limit=50, offset=offset)
        items = results['artists']['items']
        if not items:
            break
        for artist in items:
            artists.append({
                'id': artist['id'],
                'name': artist['name'],
                'popularity': artist['popularity'],
                'followers': artist['followers']['total'],
                'genres': artist['genres']
            })
        offset += 50

    return artists[:limit]

from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def get_artist_albums(artist_id):
    albums = []
    results = sp.artist_albums(artist_id, album_type='album')
    for album in results['items']:
        albums.append({
            'id': album['id'],
            'name': album['name'],
            'release_date': album['release_date'],
            'total_tracks': album['total_tracks']
        })
    return albums

from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def get_album_tracks(album_id):
    tracks = []
    results = sp.album_tracks(album_id)
    for track in results['items']:
        tracks.append({
            'id': track['id'],
            'name': track['name'],
            'duration_ms': track['duration_ms'],
            'explicit': track['explicit'],
            'track_number': track['track_number']
        })
    return tracks

from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def get_audio_features(track_ids):
    all_features = []
    chunk_size = 100

    for i in range(0, len(track_ids), chunk_size):
        chunk = track_ids[i:i + chunk_size]
        try:
            print(f"Fetching audio features for chunk {i} to {i+chunk_size}")
            features = sp.audio_features(chunk)
            all_features.extend([f for f in features if f])
        except Exception as e:
            print(f"Error fetching audio features for chunk starting at {i}: {e}")
    return all_features



def run_etl():
    print("Extracting top 100 artists...")
    artists = get_top_artists()

    with open('artists.json', 'w') as f:
        json.dump(artists, f)
    upload_file_to_s3('artists.json', 'spotify-data/artists.json')

    all_albums = []
    all_tracks = []
    all_features = []

    for artist in artists:
        albums = get_artist_albums(artist['id'])
        all_albums.extend(albums)

        for album in albums:
            tracks = get_album_tracks(album['id'])
            all_tracks.extend(tracks)

            track_ids = [track['id'] for track in tracks if track['id']]
            if track_ids:
                features = get_audio_features(track_ids)
                all_features.extend([f for f in features if f])

    with open('albums.json', 'w') as f:
        json.dump(all_albums, f)
    upload_file_to_s3('albums.json', 'spotify-data/albums.json')

    with open('tracks.json', 'w') as f:
        json.dump(all_tracks, f)
    upload_file_to_s3('tracks.json', 'spotify-data/tracks.json')

    with open('features.json', 'w') as f:
        json.dump(all_features, f)
    upload_file_to_s3('features.json', 'spotify-data/features.json')

    print("ETL complete and data uploaded to S3.")

if __name__ == "__main__":
    run_etl()
