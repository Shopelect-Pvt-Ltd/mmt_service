import logging
from pymongo import MongoClient
from fuzzywuzzy import fuzz
from datetime import datetime
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
    MMT_MATCH_COLLECTION,
)

client = MongoClient(MONGODB_CONNECTION_STRING)
mmt_database = client[MMT_DATABASE]
booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
output_collection = mmt_database[MMT_MATCH_COLLECTION]


def fetch_booking_documents():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    mmt_database = client[MMT_DATABASE]
    booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
    try:
        # query = {
        #     "$and": [
        #         {"booking_type": "HOTEL"},
        #         {"parsed_invoice": {"$exists": True}},
        #         {"match": {"$exists": False}},
        #     ]
        # }
        query = {"_id": ObjectId("667572b91c9c0ea6e5d3b660")}
        booking_documents = list(
            booking_collection.find(query).sort("_id", 1).limit(10)
        )
        print(
            f"Extracted invoice data and there are {len(booking_documents)} invoices."
        )
        return booking_documents
    except Exception as e:
        logging.error("Exception happened in fetch_booking_documents: " + str(e))


def processHotelMatch(booking):
    id = booking["_id"]
    booking_data = booking["booking_data"]
    booking_len = len(booking_data)
    invoice_data = booking["parsed_invoice"]
    invoice_len = len(invoice_data)
    print(f"No.of Booking and Invoice objects are : {booking_len} , {invoice_len}")

    matched = []
    invindexes = []
    # bookingobj gstclaimable amount is always present
    for bkindex, bookingobj in enumerate(booking_data):
        bkamount = bookingobj["GST Claimable Amount"]
        crdate = bookingobj["Created Date"]
        vendor_invoice_no = bookingobj["Vendor Invoice No"]
        customer_gstin = bookingobj["Customer GSTN"]
        match_scores = []
        for invindex, invoiceobj in enumerate(invoice_data):
            if invindex in invindexes:
                match_scores.append(0)
                continue
            invamount = invoiceobj["parsed_invoice"]["total_tax_amount"]
            invdate = invoiceobj["invoiceDate"]
            invoice_no = invoiceobj["invoiceNo"]
            guest_gstin = invoiceobj["parsed_invoice"]["guest_gstin"]

            match_output = []
            # ----- Match Scores format is [Amount, Date, Invoice Number and Customer GSTIN]
            match_output.append(simple_fuzzy_match(bkamount, invamount, "amount"))
            match_output.append(simple_fuzzy_match(crdate, invdate, "date"))
            match_output.append(
                simple_fuzzy_match(invoice_no, vendor_invoice_no, "inv_num")
            )
            match_output.append(simple_fuzzy_match(customer_gstin, guest_gstin))
            print(
                f"Match scores for Invoice obj {invindex} with Booking obj {bookingobj} is {match_output} with length {len(match_output)} "
            )
            match_output = [item for item in match_output if item is not None]
            match_scores.append(sum(match_output) / len(match_output))
            print(
                f"Processed Match scores for Invoice obj {invindex} is {match_output} with length {len(match_output)} with score {match_scores[invindex]}"
            )
            # ------------Exit Invoices-----------------
        if not max(match_scores) >= 50:
            matchedobj = {"booking": bookingobj, "invoice": None}
            matched.append(matchedobj)
            continue

        max_index = max(enumerate(match_scores), key=lambda x: x[1])[0]
        matchedobj = {"booking": bookingobj, "invoice": invoice_data[max_index]}
        invindexes.append(max_index)
        matched.append(matchedobj)
        print(f"Booking Object Date : {crdate} with best object : {max_index}")

    # ----------------Populating with leftover invoice objects--------
    if not len(invindexes) == invoice_len:
        for invindex in range(invoice_len):
            if invindex not in invindexes:
                matchedobj = {"booking": None, "invoice": invoice_data[invindex]}
                matched.append(matchedobj)

    # Need to Append to Mongo here
    # print(f"Final Object to be appended to Mongo is {matched}")
    add_to_mongo(booking, matched)


# ---------------ProcessMatch
def processMatch(booking_documents):
    if not booking_documents:
        print("No booking documents, empty collection")
        return
    for bookdoc in booking_documents:
        processHotelMatch(bookdoc)


# --------------------Fuzzy Function------------------------------
def simple_fuzzy_match(value1, value2, field_type=None):
    if not value1 or not value2:
        return None
    # -------------------------Fuzzy for Amount------------------------------
    if field_type == "amount":
        value1 = float(value1)
        value2 = float(value2)
        difference_percentage = abs(value1 - value2) / max(value1, value2) * 100
        return int(
            100 - 3 * difference_percentage
            if difference_percentage <= 15
            else (70 if difference_percentage <= 30 else 0)
        )

    # -----------------------Handling Date Exceptions--------------------------
    elif field_type == "date":
        try:
            date1 = parse_date(value1)
        except (ValueError, TypeError) as e:
            print(f"Error Parsing date for {value1} and {value2} with Error {e} \n \n")
            raise TypeError("Bad Date")
        try:
            date2 = parse_date(value2)
        except (ValueError, TypeError) as e:
            raise TypeError(
                f"Error Parsing date for {value1} and {value2} \n with Error {e} \n \n "
            )
        diff_days = abs((date1 - date2).days)
        return 100 if diff_days == 0 else (70 if diff_days <= 5 else 0)

    # ---------------------Fuzzy for String----------------------------
    elif field_type == "inv_num":
        value1, value2 = str(value1), str(value2)
        return fuzz.partial_ratio(value1.lower(), value2.lower())
    else:
        value1, value2 = str(value1), str(value2)
        return fuzz.ratio(value1, value2)


# ------------------Used to Parse Date in Fuzzy Match--------------------
def parse_date(date_str):
    date_formats = ["%H:%M %d-%b-%Y", "%Y-%m-%d %H:%M"]
    if not date_str:
        return ValueError(f"Time data {date_str} is an empty string")
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"time data '{date_str}' does not match any valid format")


# ----------------Add to Mongo----------------
def add_to_mongo(booking, matched):
    id = booking["_id"]
    # invoice_data = booking["invoice_data"]
    update_filter_criteria = {"_id": id}

    doc = {"_id": id, "match": matched}

    try:
        output_collection.insert_one(doc)
    except Exception:
        output_collection.update_one(update_filter_criteria, {"$set": doc})


if __name__ == "__main__":
    booking_documents = fetch_booking_documents()
    logging.info("No. of booking documents: " + str(len(booking_documents)))
    processMatch(booking_documents)
