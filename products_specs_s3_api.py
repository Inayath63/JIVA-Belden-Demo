import requests
from bs4 import BeautifulSoup
from fpdf import FPDF
import os
import google.generativeai as genai
import json
import re
import pandas as pd
import sys
import boto3
from botocore.exceptions import ClientError
from io import BytesIO

GEMINI_API_KEY = "AIzaSyAhaac6yPECRFwRmpifQattFvK__8YCyaY"
genai.configure(api_key=GEMINI_API_KEY)

ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

TIMEOUT_THRESHOLD = 10

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def fetch_webpage(url):
    try:
        response = requests.get(url, timeout=TIMEOUT_THRESHOLD)
        response.raise_for_status()
        return response.text
    except (requests.RequestException, requests.Timeout):
        return None

def extract_specs_with_gemini(html_content):
    model = genai.GenerativeModel('gemini-1.5-pro')
    soup = BeautifulSoup(html_content, 'html.parser')
    text_content = soup.get_text(separator='\n', strip=True)

    prompt = f"""Extract the product specifications from the following text:\n\n{text_content}\n\nReturn the specifications as a JSON object."""
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip('```json').strip().strip('```').strip()
        return json.loads(response_text)
    except Exception:
        return None

def create_pdf(specs, output_filename):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    for key, value in specs.items():
        text_line = f"{key}: {value}"
        pdf.cell(200, 10, txt=text_line.encode('latin-1', 'replace').decode('latin-1'), ln=1)

    temp_path = f"temp_{output_filename}.pdf"
    pdf.output(temp_path)
    return temp_path

def upload_to_s3(file_path, product_name, filename):
    try:
        s3_key = f"{product_name}/Product Specifications Sheets/{filename}.pdf"
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
        return True
    except ClientError:
        return False

def download_existing_pdf(url):
    try:
        response = requests.get(url, timeout=TIMEOUT_THRESHOLD)
        response.raise_for_status()
        if 'application/pdf' in response.headers.get('Content-Type', ''):
            filename = url.split('/')[-1] or 'downloaded_specs.pdf'
            with open(filename, 'wb') as f:
                f.write(response.content)
            return filename
        return None
    except (requests.RequestException, requests.Timeout):
        return None

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def scrape_product_specs(url, product_name, output_filename):
    pdf_file = download_existing_pdf(url)
    if pdf_file:
        success = upload_to_s3(pdf_file, product_name, sanitize_filename(output_filename))
        os.remove(pdf_file)
        return success

    html_content = fetch_webpage(url)
    if not html_content:
        return False

    specs = extract_specs_with_gemini(html_content)
    if not specs:
        return False

    temp_pdf_path = create_pdf(specs, output_filename)
    success = upload_to_s3(temp_pdf_path, product_name, sanitize_filename(output_filename))
    
    if os.path.exists(temp_pdf_path):
        os.remove(temp_pdf_path)
    
    return success

def read_csv_from_s3(bucket_name, s3_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read()
        df = pd.read_csv(BytesIO(csv_content))
        return df
    except (ClientError, Exception):
        return None

def process_product_from_json(json_input, bucket_name, s3_csv_key):
    try:
        input_data = json.loads(json_input)
        product_name = input_data.get("product_name", "").strip()
        
        if not product_name:
            return {"response": "error"}

        df = read_csv_from_s3(bucket_name, s3_csv_key)
        if df is None:
            return {"response": "error"}

        product_row = df[df["Product Name"].str.strip() == product_name]
        
        if product_row.empty:
            return {"response": "error"}

        row = product_row.iloc[0]
        url_columns = ["Distributor URL 1", "Distributor URL 2", 
                      "Distributor URL 3", "Distributor URL 4"]
        
        for i, url_col in enumerate(url_columns, 1):
            url = row[url_col]
            if pd.notna(url):
                output_filename = f"{product_name} {i}"
                if not scrape_product_specs(url, product_name, output_filename):
                    return {"response": "error"}

        return {"response": True}

    except Exception:
        return {"response": "error"}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"response": "error"}))
        sys.exit(1)
        
    json_input = sys.argv[1]
    s3_csv_key = "Distributor URL/Demo Distributor URLs.csv"
    
    result = process_product_from_json(json_input, BUCKET_NAME, s3_csv_key)
    print(json.dumps(result))
