import csv
import json
import boto3
from datetime import datetime

# Read the CSV
tracks_data = []

with open('dataset.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if i >= 1000:  # Take first 1000 tracks — plenty for the project
            break

        # Format it exactly like Spotify API response your pipeline expects
        track_item = {
            "added_at": "2024-01-15T10:00:00Z",
            "track": {
                "id": row["track_id"],
                "name": row["track_name"],
                "duration_ms": int(float(row["duration_ms"])),
                "popularity": int(row["popularity"]),
                "external_urls": {
                    "spotify": f"https://open.spotify.com/track/{row['track_id']}"
                },
                "album": {
                    "id": f"album_{i}",
                    "name": row["album_name"],
                    "release_date": "2024-01-01",
                    "total_tracks": 10,
                    "external_urls": {
                        "spotify": f"https://open.spotify.com/album/album_{i}"
                    },
                    "artists": [
                        {
                            "id": f"artist_{i}",
                            "name": row["artists"].split(";")[0],
                            "href": f"https://api.spotify.com/v1/artists/artist_{i}"
                        }
                    ]
                },
                "artists": [
                    {
                        "id": f"artist_{i}",
                        "name": row["artists"].split(";")[0],
                        "href": f"https://api.spotify.com/v1/artists/artist_{i}"
                    }
                ]
            }
        }
        tracks_data.append(track_item)

# Create output in exact format your pipeline expects
output = {
    "playlist": "Top Spotify Tracks Dataset",
    "tracks": len(tracks_data),
    "tracks_data": tracks_data
}

# Save locally first to verify
with open('spotify_output.json', 'w') as f:
    json.dump(output, f)

print(f"✅ Converted {len(tracks_data)} tracks successfully!")
print(f"Sample track: {tracks_data[0]['track']['name']} by {tracks_data[0]['track']['album']['artists'][0]['name']}")

# Upload to S3
s3 = boto3.client('s3', region_name='us-east-1')
filename = f"spotify_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
key = f"raw_data/to_process/{filename}"

with open('spotify_output.json', 'r') as f:
    s3.put_object(
        Bucket="spotify-etl-pipeline-soumya",
        Key=key,
        Body=f.read()
    )

print(f"✅ Uploaded to S3: {key}")
print("Now run your Transform Lambda!")
