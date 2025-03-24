import boto3
import json

AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="us-east-1"
)

def list_s3_objects(bucket_name):
    """Fetch all folders and files from the given S3 bucket."""
    objects = s3_client.list_objects_v2(Bucket=bucket_name)

    if "Contents" not in objects:
        return {"Folders": [], "Files": []}

    folders = set()
    files = []

    for obj in objects["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):
            folders.add(key)
        else:
            files.append(key)

    return {"Folders": sorted(list(folders)), "Files": sorted(files)}

s3_data = list_s3_objects(BUCKET_NAME)
print(json.dumps(s3_data, indent=4))
