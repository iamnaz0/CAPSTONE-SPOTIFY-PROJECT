import json
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
from s3_uploader import upload_file_to_s3
from tenacity import retry, wait_exponential, stop_after_attempt

# Setup Spotify client with longer timeout
sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    ),
    requests_timeout=30
)

# Get top artists
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

# Get albums by artist
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

# Get tracks by album
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

# Get audio features
@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def get_audio_features(track_ids):
    all_features = []
    chunk_size = 100
    for i in range(0, len(track_ids), chunk_size):
        chunk = track_ids[i:i + chunk_size]
        try:
            print(f" Fetching audio features for tracks {i}–{i + len(chunk)}")
            features = sp.audio_features(chunk)

            if features:
                valid_features = [f for f in features if f]
                print(f" Got {len(valid_features)} valid features")
                all_features.extend(valid_features)
            else:
                print(" Empty chunk result")
        except Exception as e:
            print(f" Error fetching features in chunk {i}: {e}")
        time.sleep(0.2)
    print(f" Total audio features collected: {len(all_features)}")
    return all_features

# Run the full ETL pipeline
def run_etl():
    print(" Starting ETL for Top 100 Artists")

    # Step 1: Extract artists
    artists = get_top_artists()
    print(f" Extracted {len(artists)} artists")
    with open('artists.json', 'w') as f:
        json.dump(artists, f)
    upload_file_to_s3('artists.json', 'spotify-data/artists.json')

    all_albums = []
    all_tracks = []
    all_features = []

    # Step 2: For each artist, get albums and tracks
    for artist in artists:
        albums = get_artist_albums(artist['id'])
        all_albums.extend(albums)

        for album in albums:
            tracks = get_album_tracks(album['id'])
            all_tracks.extend(tracks)

            track_ids = [t['id'] for t in tracks if t and 'id' in t]
            if not track_ids:
                continue

            print(f" Album {album['name']} → {len(track_ids)} track IDs")
            features = get_audio_features(track_ids)
            all_features.extend([f for f in features if f])

    # Diagnostics
    print(f" Total albums: {len(all_albums)}")
    print(f" Total tracks: {len(all_tracks)}")
    print(f" Total features: {len(all_features)}")

    # Step 3: Save and upload to S3
    with open('albums.json', 'w') as f:
        json.dump(all_albums, f)
    upload_file_to_s3('albums.json', 'spotify-data/albums.json')

    with open('tracks.json', 'w') as f:
        json.dump(all_tracks, f)
    upload_file_to_s3('tracks.json', 'spotify-data/tracks.json')

    with open('features.json', 'w') as f:
        json.dump(all_features, f)
    upload_file_to_s3('features.json', 'spotify-data/features.json')

    print(" ETL complete and all files uploaded to S3.")

# Main entry point
if __name__ == "__main__":
    run_etl()
