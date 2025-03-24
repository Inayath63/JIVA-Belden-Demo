import pandas as pd
import boto3
from io import StringIO
import json

ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

CSV_FILE_KEY = "Product Sheet/Product_sheet.csv"

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def read_csv_from_s3(bucket_name, file_key):
    """Reads CSV file from S3 and converts it to JSON format."""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj["Body"].read().decode("utf-8")

        df = pd.read_csv(StringIO(csv_data))
        
        json_data = df.to_dict(orient="list")
        return json.dumps(json_data, indent=4)

    except Exception as e:
        return json.dumps({"error": str(e)}, indent=4)

json_result = read_csv_from_s3(BUCKET_NAME, CSV_FILE_KEY)
print(json_result)
