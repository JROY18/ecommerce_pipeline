import os
import boto3
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = "D:\DE- PROJECT\ecommerc_pipeline\data"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def upload_files_s3():
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id  = AWS_ACCESS_KEY,
        aws_secret_access_key  = AWS_SECRET_KEY,
        region_name  = AWS_REGION
    )

    for filename in os.listdir(DATA_PATH):
        if filename.endswith(".csv"):
            local_path = os.path.join(DATA_PATH,filename)
            s3_path = f"raw/{filename}"

            print(f"Uploading {filename} to s3://{BUCKET_NAME}/{s3_path}...")
            s3_client.upload_file(local_path,BUCKET_NAME,s3_path)
            print(f"{filename} uploaded successfully!")

if __name__ == "__main__":
    upload_files_s3()






