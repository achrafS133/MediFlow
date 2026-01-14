import boto3
import os
from src.config import settings
from src.utils.logging_config import log

def create_buckets():
    s3 = boto3.client(
        's3',
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )
    
    buckets = ['bronze', 'silver', 'gold', 'metadata']
    
    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            log.info(f"Bucket '{bucket}' created successfully.")
        except s3.exceptions.BucketAlreadyOwnedByYou:
            log.info(f"Bucket '{bucket}' already exists.")
        except Exception as e:
            log.error(f"Error creating bucket '{bucket}': {e}")

if __name__ == "__main__":
    create_buckets()
