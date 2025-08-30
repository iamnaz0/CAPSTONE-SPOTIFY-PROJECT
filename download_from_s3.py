import boto3
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

files = ["artists.json", "albums.json", "tracks.json"]  # skip features.json for now

for file in files:
    key = f"spotify-data/{file}"
    print(f"⬇️ Downloading {key}...")
    s3.download_file(S3_BUCKET_NAME, key, file)

print("✅ All files downloaded.")
