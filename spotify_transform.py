import os
import json
import pandas as pd
import boto3
from io import BytesIO
from dotenv import load_dotenv

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET]):
    raise RuntimeError("Missing one or more AWS env vars in .env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME)")

# ---------------------------
# S3 client
# ---------------------------
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# S3 keys (paths inside bucket) for raw JSON produced by ETL
RAW_PREFIX = "spotify-data"
JSON_KEYS = {
    "artists": f"{RAW_PREFIX}/artists.json",
    "albums": f"{RAW_PREFIX}/albums.json",
    "tracks": f"{RAW_PREFIX}/tracks.json",
    # features intentionally skipped
}

# Where to upload transformed CSVs
OUTPUT_PREFIX = "output"  # will write to s3://<bucket>/output/*.csv

# ---------------------------
# Helpers
# ---------------------------
def read_json_from_s3(key: str) -> pd.DataFrame:
    """Read a JSON object from S3 and return a DataFrame (handles list or dict)."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    raw = obj["Body"].read().decode("utf-8")

    data = json.loads(raw)
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        # Fall back to flatten dict
        return pd.json_normalize(data)
    # Unknown structure -> empty frame
    return pd.DataFrame()

def upload_dataframe_csv_to_s3(df: pd.DataFrame, s3_key: str):
    """Upload a DataFrame as CSV to S3 (no temp file needed)."""
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.getvalue())
    print(f"📤 Uploaded to s3://{S3_BUCKET}/{s3_key} ({len(df)} rows)")

# ---------------------------
# Load raw JSONs from S3
# ---------------------------
artists = read_json_from_s3(JSON_KEYS["artists"])
albums  = read_json_from_s3(JSON_KEYS["albums"])
tracks  = read_json_from_s3(JSON_KEYS["tracks"])

print("✅ Loaded:")
print(" - artists:", artists.shape)
print(" - albums :", albums.shape)
print(" - tracks :", tracks.shape)

# ---------------------------
# Basic cleaning/transforms
# ---------------------------
# drop duplicates by id if present
if "id" in artists.columns: artists.drop_duplicates(subset="id", inplace=True)
if "id" in albums.columns:  albums.drop_duplicates(subset="id", inplace=True)
if "id" in tracks.columns:  tracks.drop_duplicates(subset="id", inplace=True)

# parse release_date if present
if "release_date" in albums.columns:
    albums["release_date"] = pd.to_datetime(albums["release_date"], errors="coerce")

# derive duration_minutes from duration_ms if present
if "duration_ms" in tracks.columns:
    tracks["duration_minutes"] = tracks["duration_ms"] / 60000.0

# ---------------------------
# Example aggregates (no features)
# ---------------------------
# album counts by album name (note: this is album title, not artist)
album_counts = (
    albums.groupby("name", dropna=False)
          .size()
          .reset_index(name="album_count")
)

# average track duration by track name (optional demo)
avg_durations = (
    tracks.groupby("name", dropna=False)["duration_minutes"]
          .mean()
          .reset_index(name="avg_duration_mins")
          .fillna(0.0)
)

# ---------------------------
# Save locally (optional) AND upload to S3
# ---------------------------
os.makedirs("output", exist_ok=True)
files = {
    "artists.csv": artists,
    "albums.csv": albums,
    "tracks.csv": tracks,
    "album_counts.csv": album_counts,
    "avg_durations.csv": avg_durations,
}

for filename, df in files.items():
    # local save (handy for debugging)
    local_path = os.path.join("output", filename)
    df.to_csv(local_path, index=False)
    print(f"💾 Saved {local_path} ({len(df)} rows)")

    # upload to S3
    s3_key = f"{OUTPUT_PREFIX}/{filename}"
    upload_dataframe_csv_to_s3(df, s3_key)

print("\n✅ Transformation complete.")
print(f"   CSVs are in ./output locally and in s3://{S3_BUCKET}/{OUTPUT_PREFIX}/ in S3.")
