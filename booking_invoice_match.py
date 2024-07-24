import logging
from pymongo import MongoClient
import boto3
import requests
import time
import json
import urllib.parse
import openai

from config import (
    MONGODB_CONNECTION_STRING,
    MMT_DATABASE,
    MMT_BOOKING_DATA_COLLECTION,
    S3_BUCKET_NAME,
    AWS_ACCESS_KEY_ID,
    AWS_ACCESS_SECRET_KEY,
    AWS_REGION,
    ENDPOINT,
    SUBSCRIPTION_KEY,
    AZURE_OPENAI_API_KEY,
    OPENAI_KEY
)

openai_headers = {
    'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
    'Content-Type': 'application/octet-stream'
}

openai.api_key = AZURE_OPENAI_API_KEY

analyze_url = f"{ENDPOINT}vision/v3.1/read/analyze"

def fetch_booking_documents():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    mmt_database = client[MMT_DATABASE]
    booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
    try:
        query = {'booking_type': 'HOTEL'}
        booking_documents = list(booking_collection.find(query).limit(1000))
        print(f"Extracted invoice data and there are {len(booking_documents)} invoices.")
        return booking_documents
    except Exception as e:
        logging.info("Exception happened in fetch_booking_documents: " + str(e))

def format_duration(seconds):
    return f"{seconds:.1f} s"

def extract_text_from_vision_api(file_contents):
    try:
        start_time = time.time()
        response = requests.post(analyze_url, headers=openai_headers, data=file_contents)
        response.raise_for_status()
        if response.status_code == 202:
            operation_url = response.headers["Operation-Location"]
            while True:
                response_status = requests.get(operation_url, headers=openai_headers)
                status = response_status.json()
                if 'status' in status and status['status'] == 'succeeded':
                    break
                time.sleep(1)
            extracted_text = ''
            for result in status['analyzeResult']['readResults']:
                for line in result['lines']:
                    extracted_text += line['text'] + " "
            end_time = time.time()
            duration = format_duration(end_time - start_time)
            return extracted_text, "succeeded", duration
        else:
            return None, "failed", "0.0 s"
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None, "failed", "0.0 s"
    except KeyError as e:
        print("KeyError:", e)
        return None, "failed", "0.0 s"

def get_s3_object_content(s3_url):
    try:
        parsed_url = urllib.parse.urlparse(s3_url)
        bucket_name = parsed_url.netloc.split('.')[0]

        if bucket_name is None:
            bucket_name = S3_BUCKET_NAME

        object_key = parsed_url.path.lstrip('/')

        # Create a session using Boto3
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_SECRET_KEY,
            region_name=AWS_REGION
        )
        # Create an S3 client
        s3_client = session.client('s3')

        # Fetch the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read the content of the object
        content = response['Body'].read()
        return content
    except Exception as e:
        logging.info("Exception happened in get_s3_object_content: " + str(e))
        return None

def openai_chat_completion(messages):
    openai_client = openai.OpenAI(api_key=OPENAI_KEY)

    try:
        start_time = time.time()
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            response_format={"type": "json_object"}
        )
        end_time = time.time()
        duration = format_duration(end_time - start_time)
        response_content = response.choices[0].message.content
        return response_content, "success", duration
    except Exception as e:
        print("Error querying API:", e)
        return None, "failed", "0.0 s"

def truncate_text(text, max_length):
    if len(text) > max_length:
        return text[:max_length]
    return text

def preprocess_text(text):
    lines = text.splitlines()
    relevant_lines = [line for line in lines if "GSTIN" in line or "invoice" in line]
    return " ".join(relevant_lines)

def process_text_with_openai(extracted_text, system_message):
    max_length = 128000 - len(system_message)  # Adjust based on the actual system message length
    preprocessed_text = preprocess_text(extracted_text)
    truncated_text = truncate_text(preprocessed_text, max_length)
    prompt = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": f"Please map the details found in the following text to the relevant keys and give me a JSON output: {truncated_text}"}
    ]
    response_content, status, duration = openai_chat_completion(prompt)
    if status == "success":
        try:
            main = json.loads(response_content)
            return main, status, duration
        except (ValueError, SyntaxError) as e:
            logging.error("Error parsing response content: %s", e)
            return None, "failed", duration
    return None, status, duration

def processHotel(booking):
    if "invoice_data" in booking and "booking_data" in booking:
        booking_data = booking["booking_data"]
        invoice_data = booking["invoice_data"]
        invoiceurls = []
        for i in range(len(invoice_data)):
            if "invoiceTypeWiseData" in invoice_data[i] and "GST" in invoice_data[i]["invoiceTypeWiseData"]:
                for j in range(len(invoice_data[i]["invoiceTypeWiseData"]["GST"])):
                    if "invoiceUrl" in invoice_data[i]["invoiceTypeWiseData"]["GST"][j]:
                        if "airline-engine-scraped" in invoice_data[i]["invoiceTypeWiseData"]["GST"][j]["invoiceUrl"]:
                            invoiceurls.append(invoice_data[i]["invoiceTypeWiseData"]["GST"][j]["invoiceUrl"])

        if len(invoiceurls) != 0:
            for j in range(len(invoiceurls)):
                print(booking_data)
                print(invoiceurls[j])
                file_content = get_s3_object_content(invoiceurls[j])
                if file_content:
                    extracted_text, status, duration = extract_text_from_vision_api(file_content)
                    print(status)
                    if status == "succeeded":
                        system_message = """
                            You are an expert at finding data patterns from a string and mapping them to keys. The text provided will be extracted from an image of a hotel invoice, containing information such as buyer details (buyer name, GSTIN, address, and contact details), seller details (GSTIN, address, etc.), invoice details (number, date, amount), item table, GST rate, amount, etc.

                            GSTIN is GST number which is 15 digit GST code.
                            GST rate can only be 5%, 12%, 18% or 28%. No other rate is applicable in India. If you see, 6, 9, or 14, its split of GST into CGST, SGST. Ignore that!

                            Match the values to the following keys:

                            gst_amount
                            gst_rate
                            hotel_address
                            hotel_name
                            buyer_name
                            seller_phone
                            guest_gstin
                            invoice_date
                            total_tax_amount
                            total_tax_%
                            subtotal
                            invoice_amount
                            hotel_gstin
                            invoice_number or bill_number as invoice_number
                            hotel_email

                            If the invoice doesn't have a specific label, use its alias if available. Format all dates in the DD/MM/YYYY format. Give amounts in python number format, not string. Do not print any extra strings or symbols than asked for.
                        """
                        result, status, duration = process_text_with_openai(extracted_text, system_message)
                        logging.info(result)
                        print(result)
                    else:
                        logging.error(f"Text extraction failed for URL: {invoiceurls[j]}")

def processData(booking_documents):
    for booking in booking_documents:
        if booking["booking_type"] == "HOTEL":
            processHotel(booking)

if __name__ == '__main__':
    booking_data = fetch_booking_documents()
    logging.info("No. of booking documents: " + str(len(booking_data)))
    processData(booking_data)






