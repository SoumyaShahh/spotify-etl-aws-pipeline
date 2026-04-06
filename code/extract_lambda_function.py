import json
import boto3
import urllib.request
import urllib.parse
import base64
import os
from datetime import datetime


def get_spotify_token(client_id, client_secret):
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
    req = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=data,
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
    )
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read())["access_token"]


def get_playlist_tracks(token, playlist_id):
    all_tracks = []
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?limit=100"

    while url:
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"}
        )
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read())
                all_tracks.extend(data["items"])
                url = data.get("next")
        except Exception as e:
            print(f"Error fetching tracks: {e}")
            break

    return all_tracks


def get_playlist_name(token, playlist_id):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}?fields=name"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read())["name"]


def lambda_handler(event, context):
    try:
        print("Spotify ETL Pipeline Starting...")

        client_id = os.environ['client_id']
        client_secret = os.environ['client_secret']

        # Get token
        token = get_spotify_token(client_id, client_secret)
        print("Token obtained successfully")

        playlist_id = "5ABHKGoOzxkaa28ttQV9sE"

        # Get tracks directly via API
        all_tracks = get_playlist_tracks(token, playlist_id)
        tracks_count = len(all_tracks)
        print(f"Extracted {tracks_count} tracks")

        # Get playlist name
        playlist_name = get_playlist_name(token, playlist_id)

        output = {
            "playlist": playlist_name,
            "tracks": tracks_count,
            "tracks_data": all_tracks
        }

        s3 = boto3.client("s3")
        filename = f"spotify_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        key = f"raw_data/to_process/{filename}"

        s3.put_object(
            Bucket="spotify-etl-pipeline-soumya",
            Key=key,
            Body=json.dumps(output)
        )

        print(f"Uploaded to S3: {key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "SUCCESS!",
                "playlist": playlist_name,
                "tracks": tracks_count,
                "s3_file": key
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
