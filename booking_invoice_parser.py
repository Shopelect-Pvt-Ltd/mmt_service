from logger import logging
from pymongo import MongoClient
import boto3
import requests
import time
import json
import urllib.parse
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
from bson import ObjectId
import threading

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
    OPENAI_KET,
    system_message,
)

openai_headers = {
    "Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY,
    "Content-Type": "application/octet-stream",
}

openai.api_key = AZURE_OPENAI_API_KEY

analyze_url = f"{ENDPOINT}vision/v3.1/read/analyze"

# --------------Mongo Connections --------------
client = MongoClient(MONGODB_CONNECTION_STRING)
mmt_database = client[MMT_DATABASE]
booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
output_collection = booking_collection


def fetch_booking_documents():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    mmt_database = client[MMT_DATABASE]
    booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
    try:
        query = {
            "$and": [
                {"booking_type": "HOTEL"},
                {"invoice_data.invoiceTypeWiseData.GST.invoiceUrl": {"$exists": True}},
                {
                    "invoice_data.invoiceTypeWiseData.GST.invoiceUrl": {
                        "$regex": "airline-engine-scraped"
                    }
                },
            ]
        }
        # query = {"_id": ObjectId("667571cf4ec3e000a5eeb976")}
        booking_documents = list(
            booking_collection.find(query).sort("_id", 1).limit(900)
        )
        print(
            f"Extracted invoice data and there are {len(booking_documents)} invoices."
        )
        return booking_documents
    except Exception as e:
        logging.error("Exception happened in fetch_booking_documents: " + str(e))


def format_duration(seconds):
    return f"{seconds:.1f} s"


def extract_text_from_vision_api(file_contents):
    try:
        start_time = time.time()
        response = requests.post(
            analyze_url, headers=openai_headers, data=file_contents
        )
        response.raise_for_status()
        if response.status_code == 202:
            operation_url = response.headers["Operation-Location"]
            while True:
                response_status = requests.get(operation_url, headers=openai_headers)
                status = response_status.json()
                if "status" in status and status["status"] == "succeeded":
                    break
                time.sleep(1)
            extracted_text = ""
            for result in status["analyzeResult"]["readResults"]:
                for line in result["lines"]:
                    extracted_text += line["text"] + " "
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
        bucket_name = parsed_url.netloc.split(".")[0]

        if bucket_name is None:
            bucket_name = S3_BUCKET_NAME

        object_key = parsed_url.path.lstrip("/")

        # Create a session using Boto3
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_SECRET_KEY,
            region_name=AWS_REGION,
        )
        # Create an S3 client
        s3_client = session.client("s3")

        # Fetch the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read the content of the object
        content = response["Body"].read()
        return content
    except Exception as e:
        logging.error("Exception happened in get_s3_object_content: " + str(e))
        return None


def openai_chat_completion(messages):
    openai_client = openai.OpenAI(api_key=OPENAI_KET)

    try:
        start_time = time.time()
        response = openai_client.chat.completions.create(
            model="gpt-4o", messages=messages, response_format={"type": "json_object"}
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


def process_text_with_openai(extracted_text, system_message):
    max_length = 128000 - len(
        system_message
    )  # Adjust based on the actual system message length
    truncated_text = truncate_text(extracted_text, max_length)
    prompt = [
        {"role": "system", "content": system_message},
        {
            "role": "user",
            "content": f"Please map the details found in the following text to the relevant keys and give me a JSON output: {truncated_text}",
        },
    ]
    response_content, status, duration = openai_chat_completion(prompt)
    if status == "success":
        try:
            main = json.loads(response_content)
            # with open("extracted_text.json", "a") as jsonfile:
            #     jsonfile.write(f"Output: {main} \n")
            return main, status, duration
        except (ValueError, SyntaxError) as e:
            logging.error("Error parsing response content: %s", e)
            return None, "failed", duration
    return None, status, duration


def processHotel(docindex, booking, total_documents, lock):
    if "invoice_data" in booking and "booking_data" in booking:
        booking_data = booking["booking_data"]
        invoice_data = booking["invoice_data"]

        if not isinstance(invoice_data, list):
            print(f"Invoice Data does not exist for BookingID : {booking["bookingId"]}")
            return

        gst_object_path = invoice_data[0]["invoiceTypeWiseData"]["GST"]
        parsed_invoices = []
        print(
            f"There are {len(gst_object_path)} GST Invoices for Document index {docindex}"
        )
        for index in range(len(gst_object_path)):
            invoiceUrl = gst_object_path[index]["invoiceUrl"]
            if "airline-engine-scraped" not in invoiceUrl:
                continue

            # print(booking_data)
            print(invoiceUrl)
            file_content = get_s3_object_content(invoiceUrl)
            if file_content:
                extracted_text, status, duration = extract_text_from_vision_api(
                    file_content
                )
                print(status)
                if status == "succeeded":
                    parsed_invoice, status, duration = process_text_with_openai(
                        extracted_text, system_message
                    )
                    # logging.info(parsed_invoice)
                    tempdict = gst_object_path[index]
                    tempdict["parsed_invoice"] = parsed_invoice
                    # print(tempdict)
                    parsed_invoices.append(tempdict)

                else:
                    logging.error(f"Text extraction failed for URL: {invoiceUrl}")
        # print(parsed_invoices)
        logging.info(parsed_invoices)
        add_to_mongo(booking, output_collection, parsed_invoices)
        # print(f"----------{docindex}/{total_documents} Completed-------------")


def processData(booking_documents):
    if not booking_documents:
        print("No booking documents, empty collection")
        return

    lock = threading.Lock()

    counter = 1
    total_documents = len(booking_documents)
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = [
            executor.submit(processHotel, docindex, booking, total_documents, lock)
            for docindex, booking in enumerate(booking_documents)
        ]
        for future in futures:
            # Wait till all threads complete
            try:
                future.result()
                print(f"------------{counter}/{total_documents} Commpleted---------")
                counter += 1
            except Exception as e:
                print(f"A thread failed due to {str(e)}")


def add_to_mongo(booking, output_collection, parsed_invoices):
    id = booking["_id"]
    invoice_data = booking["invoice_data"]
    update_filter_criteria = {"_id": id}

    doc = {"_id": id, "parsed_invoice": parsed_invoices}

    try:
        output_collection.insert_one(doc)
    except Exception:
        output_collection.update_one(update_filter_criteria, {"$set": doc})


if __name__ == "__main__":
    booking_documents = fetch_booking_documents()
    logging.info("No. of booking documents: " + str(len(booking_documents)))
    processData(booking_documents)
