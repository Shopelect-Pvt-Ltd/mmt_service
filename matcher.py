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
    MMT_TEST_COLLECTION,
)

client = MongoClient(MONGODB_CONNECTION_STRING)
mmt_database = client[MMT_DATABASE]
booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
# output_collection = mmt_database[MMT_MATCH_COLLECTION]
output_collection = mmt_database[MMT_TEST_COLLECTION]


def fetch_booking_documents():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    mmt_database = client[MMT_DATABASE]
    booking_collection = mmt_database[MMT_BOOKING_DATA_COLLECTION]
    try:
        query = {
            "$and": [
                {"booking_type": "HOTEL"},
                {"parsed_invoice": {"$exists": True}},
            ]
        }
        # query = {"_id": ObjectId("66758d644ec3e000a5eec22a")}
        booking_documents = list(booking_collection.find(query).sort("_id", 1))
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

    match_scores = {}
    matched = []
    invindexes = []
    bkindexes = []
    # bookingobj gstclaimable amount is always present
    for bkindex, bookingobj in enumerate(booking_data):
        bkamount = bookingobj["GST Claimable Amount"]
        crdate = bookingobj["Created Date"]
        vendor_invoice_no = bookingobj["Vendor Invoice No"]
        customer_gstin = bookingobj["Customer GSTN"]

        for invindex, invoiceobj in enumerate(invoice_data):
            if invindex in invindexes:
                match_scores.append(0)
                continue
            invdate = invoiceobj["invoiceDate"]
            invoice_no = invoiceobj["invoiceNo"]
            print(f"Invoice Object as {invoiceobj}")

            try:
                invamount = invoiceobj["parsed_invoice"]["total_tax_amount"]
            except Exception as e:
                logging.error(f"Error Parsing Tax Amount as {str(e)}")
                invamount = None

            try:
                guest_gstin = invoiceobj["parsed_invoice"]["guest_gstin"]
            except Exception as e:
                logging.error(f"Error Parsing Guest_gstin as {str(e)}")
                guest_gstin = None

            match_output = []
            # ----- Match Scores format is [Amount, Date, Invoice Number and Customer GSTIN]
            match_output.append(simple_fuzzy_match(bkamount, invamount, "amount"))
            match_output.append(simple_fuzzy_match(crdate, invdate, "date"))
            match_output.append(
                simple_fuzzy_match(invoice_no, vendor_invoice_no, "inv_num")
            )
            match_output.append(simple_fuzzy_match(customer_gstin, guest_gstin))
            print(
                f"Match scores for Invoice obj {invindex} with Booking obj {bkindex} is {match_output} with length {len(match_output)} "
            )
            match_output = [item for item in match_output if item is not None]
            var_name = f"{bkindex}B-{invindex}I"
            match_score = sum(match_output) / len(match_output)
            match_scores[var_name] = match_score
            # print(
            #     f"Processed Match scores for Invoice obj {invindex} is {match_output} with length {len(match_output)} with score {match_scores[invindex]}"
            # )
            # ------------Exit Invoices-----------------

    print(f"Match Scores = {match_scores}")
    max_key = max(match_scores, key=match_scores.get)
    max_value = match_scores[max_key]
    print(f"Max Match Score {max_value} on key {max_key}")
    remove_parts = max_key.split("-")
    # --------Populate matched------------
    bkindexpop = int(remove_parts[0][:1])
    bkindexes.append(bkindexpop)
    invindexpop = int(remove_parts[1][:1])
    invindexes.append(invindexpop)
    matchedobj = {
        "booking": booking_data[bkindexpop],
        "invoice": invoice_data[invindexpop],
    }
    matched.append(matchedobj)
    filtered_scores = {
        key: value
        for key, value in match_scores.items()
        if all(part not in key for part in remove_parts)
    }
    while filtered_scores:
        max_key = max(filtered_scores, key=filtered_scores.get)
        max_value = match_scores[max_key]
        if not max_value >= 50:
            break
        print(f"Max Match Score {max_value} on key {max_key}")
        remove_parts = max_key.split("-")
        # ---------Populate matched---------------
        bkindexpop = int(remove_parts[0][:1])
        bkindexes.append(bkindexpop)
        invindexpop = int(remove_parts[1][:1])
        invindexes.append(invindexpop)
        matchedobj = {
            "booking": booking_data[bkindexpop],
            "invoice": invoice_data[invindexpop],
        }
        matched.append(matchedobj)
        filtered_scores = {
            key: value
            for key, value in filtered_scores.items()
            if all(part not in key for part in remove_parts)
        }
        print("Filtered Match Scores:", filtered_scores)
    print(f"Leftover Dictionary:{filtered_scores} ")
    # -------Handling leftover dict------
    while filtered_scores:
        temp_key = list(filtered_scores.keys())[0]
        remove_parts = temp_key.split("-")
        # ---------Populate matched---------------
        bkindexpop = int(remove_parts[0][:1])
        bkindexes.append(bkindexpop)
        invindexpop = int(remove_parts[1][:1])
        invindexes.append(invindexpop)
        matchedobj = {
            "booking": booking_data[bkindexpop],
            "invoice": None,
        }
        matched.append(matchedobj)
        matchedobj = {
            "booking": None,
            "invoice": invoice_data[invindexpop],
        }
        matched.append(matchedobj)
        # ------Remove Keys--------
        filtered_scores = {
            key: value
            for key, value in filtered_scores.items()
            if all(part not in key for part in remove_parts)
        }
    print(
        f"Dictionary Complete with BK indexes {bkindexes} and InvIndexes {invindexes}"
    )

    # ----------------Populating with leftover invoice and booking objects--------
    if not len(invindexes) == invoice_len:
        for invindex in range(invoice_len):
            if invindex not in invindexes:
                matchedobj = {"booking": None, "invoice": invoice_data[invindex]}
                matched.append(matchedobj)

    if not len(bkindexes) == booking_len:
        for bkindex in range(booking_len):
            if bkindex not in invindexes:
                matchedobj = {"booking": booking_data[bkindex], "invoice": None}
                matched.append(matchedobj)

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
        if value1 > 0 or value2 > 0:
            difference_percentage = abs(value1 - value2) / max(value1, value2) * 100
            return int(
                100 - 3 * difference_percentage
                if difference_percentage <= 15
                else (70 if difference_percentage <= 30 else 0)
            )
        else:
            return None
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
