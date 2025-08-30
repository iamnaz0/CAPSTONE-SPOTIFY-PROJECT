import boto3
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME

def upload_file_to_s3(local_file_path, s3_file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )                                                                                                                                                                                                                                                                                                                                                                                                       
    try:
        s3.upload_file(local_file_path, S3_BUCKET_NAME, s3_file_path)
        print(f"Uploaded {local_file_path} to s3://{S3_BUCKET_NAME}/{s3_file_path}")
    except Exception as e:
        print(f"Error uploading file: {e}")
