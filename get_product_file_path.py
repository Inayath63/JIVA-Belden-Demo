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

def get_csv_path(bucket_name, file_key):
    """Fetch the specified CSV file path from the S3 bucket."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        s3_path = f"s3://{bucket_name}/{file_key}"
        return {"status": "success", "path": s3_path}
    except s3_client.exceptions.ClientError:
        return {"status": "error", "path": None}

file_key = "Product Sheet/Product_sheet.csv"

result = get_csv_path(BUCKET_NAME, file_key)

print(json.dumps(result, indent=4))
