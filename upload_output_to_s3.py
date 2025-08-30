import os
from s3_uploader import upload_file_to_s3

output_dir = "./output"
s3_prefix = "output"

for file in os.listdir(output_dir):
    if file.endswith(".csv"):
        local_path = os.path.join(output_dir, file)
        s3_key = f"{s3_prefix}/{file}"
        print(f"📤 Uploading {file} to s3://s3-spotify-pro/{s3_key}")
        upload_file_to_s3(local_path, s3_key)

print("✅ Upload complete.")
