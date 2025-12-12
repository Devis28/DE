import os
import boto3
from dotenv import load_dotenv

load_dotenv()

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_KEY_ID = os.getenv("R2_KEY_ID")
R2_SECRET = os.getenv("R2_SECRET")
R2_BUCKET = os.getenv("R2_BUCKET")

session = boto3.session.Session()
r2 = session.client(
    service_name="s3",
    aws_access_key_id=R2_KEY_ID,
    aws_secret_access_key=R2_SECRET,
    endpoint_url=R2_ENDPOINT,
)

def upload_file(local_path, r2_object_path):
    with open(local_path, "rb") as data:
        r2.upload_fileobj(data, R2_BUCKET, r2_object_path)
