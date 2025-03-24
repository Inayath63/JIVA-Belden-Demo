import pandas as pd
from googlesearch import search
import time
import random
import requests
import boto3
from botocore.exceptions import ClientError
import os
from io import BytesIO, StringIO
import json
import sys

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0'
]


ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

INPUT_S3_KEY = "Product Sheet/Product_sheet.csv"
OUTPUT_S3_KEY = "Product Sheet/Product_sheet.csv"

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def get_first_search_result(part_number):
    try:
        search_query = f"Lumberg Automation {part_number} pdf filetype:pdf"
        results = search(search_query, num_results=1, lang="en", advanced=False)
        first_result = next(results, None)
        if first_result and 'google' not in first_result.lower():
            return first_result
        return "No PDF found"
    except Exception as e:
        print(f"Error searching for {part_number}: {str(e)}")
        return "Search error"

def download_and_upload_pdf(url, part_number):
    """Download PDF and upload to S3"""
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        if 'application/pdf' in response.headers.get('Content-Type', ''):
            filename = url.split('/')[-1] if '.pdf' in url.split('/')[-1] else f"{part_number}_datasheet.pdf"
            temp_path = f"temp_{filename}"
            with open(temp_path, 'wb') as f:
                f.write(response.content)

            s3_key = f"{part_number}/Belden Datasheet/{filename}"
            s3_client.upload_file(temp_path, BUCKET_NAME, s3_key)
            os.remove(temp_path)
            # print(f"Uploaded PDF to s3://{BUCKET_NAME}/{s3_key}")
            return url
        else:
            print(f"URL {url} did not return a PDF")
            return "Not a PDF"
    except (requests.RequestException, ClientError) as e:
        print(f"Error downloading/uploading {url}: {str(e)}")
        return "Download/Upload error"
    except Exception as e:
        print(f"Unexpected error with {url}: {str(e)}")
        return "Error"

def read_csv_from_s3(bucket_name, s3_key):
    """Read CSV file from S3 into a pandas DataFrame"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read()
        df = pd.read_csv(BytesIO(csv_content))
        return df
    except ClientError as e:
        print(f"Error reading CSV from S3: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error reading CSV from S3: {e}")
        return None

def save_df_to_s3(df, bucket_name, s3_key):
    """Save DataFrame to S3 as CSV"""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_bytes,
            ContentType='text/csv'
        )
        # print(f"Updated CSV saved to s3://{bucket_name}/{s3_key}")
        return True
    except ClientError as e:
        print(f"Error saving CSV to S3: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error saving CSV to S3: {e}")
        return False

def process_data(json_input):
    try:
        # Parse JSON input
        input_data = json.loads(json_input)
        product_name = input_data.get("product_name", "").strip()
        
        if not product_name:
            print("Error: Product name not provided in JSON input")
            return {"response": "error"}

        # Read CSV from S3
        df = read_csv_from_s3(BUCKET_NAME, INPUT_S3_KEY)
        if df is None:
            print("Failed to read CSV from S3.")
            return {"response": "error"}

        # Ensure 'Belden Data Sheet URL' column exists and is string type
        if 'Belden Data Sheet URL' not in df.columns:
            df['Belden Data Sheet URL'] = pd.Series(dtype='string')
        else:
            df['Belden Data Sheet URL'] = df['Belden Data Sheet URL'].astype('string')

        # Filter DataFrame for the specified product name
        product_row = df[df['part_number'].str.strip() == product_name]
        if product_row.empty:
            print(f"Error: Product '{product_name}' not found in CSV")
            return {"response": "error"}

        # Process only the matching row
        index = product_row.index[0]
        row = product_row.iloc[0]
        part_number = row['part_number']
        # print(f"Processing part number: {part_number}")

        url = get_first_search_result(part_number)
        # print(f"Found URL: {url}")
        
        if url not in ["No PDF found", "Search error"]:
            result = download_and_upload_pdf(url, part_number)
            df.at[index, 'Belden Data Sheet URL'] = result
        else:
            df.at[index, 'Belden Data Sheet URL'] = url

        # Save updated DataFrame to S3
        if not save_df_to_s3(df, BUCKET_NAME, OUTPUT_S3_KEY):
            print("Failed to save updated CSV to S3.")
            return {"response": "error"}

        return {"response": True}

    except json.JSONDecodeError as e:
        # print(f"Error: Invalid JSON input - {str(e)}")
        return {"response": "error"}
    except Exception as e:
        # print(f"Unexpected error in processing: {str(e)}")
        return {"response": "error"}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # print("Error: Please provide a JSON input with product name as a command line argument")
        # print("Usage: python script.py '{\"product_name\": \"0911 ANC 410\"}'")
        print(json.dumps({"response": "error"}))
        sys.exit(1)

    json_input = sys.argv[1]
    result = process_data(json_input)
    print(json.dumps(result))
