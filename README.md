# JIVA-Belden-Demo

1) **products_specs_s3_api.py** - API Type: Get & Put
Description: This script automates the process of scraping product specifications from distributor URLs, generating PDF files, and uploading them to an AWS S3 bucket. It takes a JSON input containing a product name, retrieves corresponding URLs from a CSV file stored in S3, and then:
1.	Attempts to download existing PDF spec sheets from the URLs.
2.	If no PDF is found, it fetches the webpage, extracts specs using Google's Gemini AI model, and creates a PDF.
3.	Uploads the resulting PDF to an S3 bucket under a product-specific folder.
4.	Handles errors and cleans up temporary files.

The process supports multiple distributor URLs per product and returns a JSON response indicating success or failure.

Input: "{\"product_name\": \"0911 ANC 410\"}"

Output: {"response": true}

2) **get_belden_datasheets** - API Type: Get
Description: This process takes a JSON input with a product name via command-line argument, reads a CSV file (Product_sheet.csv) from an S3 bucket (belden-demo-bucket), filters it for the specified product, searches for a PDF datasheet using Google, downloads and uploads it to S3 under a product-specific folder, updates the CSV with the PDF URL, and saves it back to S3 as Product_sheet_updated.csv. It outputs {"response": true} on success or {"response": "error"} on failure.

Input: "{\"product_name\": \"0935 S4711 301\"}"
Output: {"response": true}

3) **get_product_details** - Description: This process retrieves a CSV file (Product_sheet.csv) from an S3 bucket (belden-demo-bucket) using boto3, converts it to a pandas DataFrame, transforms the data into a JSON format with columns as keys and values as lists, and prints the result. Errors are returned as a JSON "error" message.

API Type: Get
Result JSON: {
    "part_number": [
        "0911 ANC 410",
        "0935 S4711 301/...M",
        "0955 284 201",
        "RSWU 12-RKWU 12-256/...M",
        "RKT 3U-618/...F",
        "ASB 2-RKT 4-3-632/...M",
        "RSRK 901M-623/...F",
        "RSMV 3-224/...M",
        "RST 5-RKM 5-507/...M",
        "0975 254 101/...M",
        "GAN22LU-V24-226"
    ],
    "web_part_number": [
        "0911 ANC 410",
        "0935 S4711 301",
        "0955 284 201",
        "RSWU 12-RKWU 12-256",
        "RKT 3U-618",
        "ASB 2-RKT 4-3-632",
        "RSRK 901M-623",
        "RSMV 3-224",
        "RST 5-RKM 5-507",
        "0975 254 101",
        "GAN22LU-V24-2260060"
    ],
    "product_group": [
        "AS-Interface",
        "Devicenet",
        "Interbus",
        "M23",
        "Micro (1/2-20)",
        "Micro (M12)",
        "Mini",
        "Pico (M8)",
        "M8 Power",
        "Profibus",
        "Din Valve"
    ],
    "Belden Data Sheet URL": [
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN
    ],
    "Distributor 1 URL": [
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN
    ],
    "Distributor 2 URL": [
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN
    ],
    "Distributor 3 URL": [
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN,
        NaN
    ]
}


4) **get_product_file_path** - API Type: Get
Description: This process connects to the "belden-demo-bucket" S3 bucket using provided AWS credentials, checks for the existence of "Product Sheet/Product_sheet.csv", and returns its full S3 path (e.g., "s3://belden-demo-bucket/Product Sheet/Product_sheet.csv") in a JSON response. If the file isn't found, it returns an error status.

Input: Not Required
Output JSON: {
    "status": "success",
    "path": "s3://belden-demo-bucket/Product Sheet/Product_sheet.csv"
}

5) **get_s3_bucket_details** - API Type: Get
Description: This script connects to an AWS S3 bucket using provided credentials, lists all objects, and categorizes them into folders (keys ending with "/") and files. It then returns the sorted lists in a JSON format, displaying the bucket's contents.

Input: Not Required
Output JSON: {
    "Folders": [
        "0911 ANC 410/",
        "0911 ANC 410/Distributor Product Datasheet/",
        "0911 ANC 410/Product Datasheet/"
    ],
    "Files": [
        "0911 ANC 410/Distributor Product Datasheet/accu-tech_0975 254 101_distributor.pdf",
        "0911 ANC 410/Distributor Product Datasheet/rs-online_0975 254 101_distributor.pdf",
        "0911 ANC 410/Product Datasheet/Product_Manual_0935 S4711 301.pdf"
    ]
}


