import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import json
from bson import ObjectId
import time

# --------------- Function Imports---------------
from common_functions import (
    simple_fuzzy_match,
    format_date_column_2b,
    clean_amount,
    convert_to_float,
    retrieve_row,
    is_empty,
    fetch_irn_match,
)


# Config imports
from config import (
    MONGODB_CONNECTION_STRING,
    MMT_DATABASE,
    MMT_BOOKING_DATA_COLLECTION,
    MMT_MATCH_COLLECTION,
    MMT_PAN_LIST,
    DATABASE_GST,
    COLLECTION_GST_INPUT_2B,
    COLLECTION_GST_INPUT_6A,
    COLLECTION_CSV,
    DATABASE_FUZZY,
    MMT_GST_MATCH_COLLECTION,
    IRN_FLAT_COLLECTION,
)

global invoicecount
invoicecount = 0


# ----------------- Connect to MongoDB---------------
# MMT
client = MongoClient(MONGODB_CONNECTION_STRING)
mmt_database = client[MMT_DATABASE]
booking_collection = mmt_database[MMT_GST_MATCH_COLLECTION]
# output_collection = mmt_database[MMT_MATCH_COLLECTION]
# output_collection = mmt_database[MMT_TEST_COLLECTION]
output_collection = mmt_database[MMT_GST_MATCH_COLLECTION]

# GST
database_gst = client[DATABASE_GST]
collection_2b = database_gst[COLLECTION_GST_INPUT_2B]
collection_6a = database_gst[COLLECTION_GST_INPUT_6A]
database_fuzzy = client[DATABASE_FUZZY]
collection_csv = database_fuzzy[COLLECTION_CSV]
irn_collection = database_gst[IRN_FLAT_COLLECTION]


# --------------------Fetch data from GST depending on Pan_list--------------------------
def fetch_documents_by_gstin_and_load_to_dataframe(pan_list, gsttable):
    all_2b = []

    # gsttable = "2b"
    if gsttable == "2b":
        for pan in pan_list:
            query = {
                "$and": [
                    {
                        "vendorType": {"$not": {"$eq": "Air"}},
                        "items.rt": 12,
                        "gstin": {"$regex": pan},
                    }
                ]
            }
            documents_2b = list(collection_2b.find(query))
            all_2b += documents_2b
            print(f"no. of 2b for the pan {pan} are {len(documents_2b)}")
        df = pd.DataFrame(all_2b)
        format_date_column_2b(df, "dt")

    return df


# ----------------Process Single Document--------------------------
def process_single_document(bookDoc, df_gst, invindex, good_list, df_2b):
    print("ENTER Processing Single Document")

    ID = bookDoc["_id"]
    booking_data = bookDoc["booking"]
    invoice_data = bookDoc["invoice"]
    parsed_invoice_data = invoice_data["parsed_invoice"]
    # print(list(invoice_data.keys()))
    required_keys = [
        "invoice_number",
        "invoice_date",
        "invoice_amount",
        "guest_gstin",
    ]
    required_keys_gst = [
        "invoice_number",
        "invoice_date",
        "invoice_value",
        "supplier_gstin",
        "buyer_gstin",
        "gsttable",
    ]

    if not invoice_data:
        print(f"Missing invoice data for index {invindex}")
        return

    # -----------------Further Filtering based on range of amount---------------
    try:
        amount = float(parsed_invoice_data["invoice_amount"])
        # print(f"Amount Type : {type(amount)}")
        filtered_df = df_gst[
            (df_gst[required_keys_gst[2]] > (0.8 * amount))
            & (df_gst[required_keys_gst[2]] < (1.2 * amount))
        ]
        # print(f"Filtered_df :{filtered_df}")
    except (ValueError, TypeError) as e:
        print(
            f"Total Tax Amount is a non-parseable value: {parsed_invoice_data["invoice_amount"]} due to error {str(e)}"
        )
        return
    # print(f"Shape of filtered 2b = {filtered_2b.shape} and Index is {invindex}")

    # ----------------Parameters for frontend Invoice Table - Reducing InvoiceData to 5 columns---------------------
    invoice_details = {
        "invoice_amount": parsed_invoice_data["invoice_amount"],
        "guest_gstin": parsed_invoice_data["guest_gstin"],
        "invoice_number": invoice_data["invoiceNo"],
        "invoice_date": invoice_data["invoiceDate"],
        # parsed_invoice_data["invoice_number"]
    }

    global invoicecount
    invoicecount += filtered_df.shape[0]
    print(f"Invoices to iterate fuzzy = {filtered_df.shape[0]} for Index {invindex}")

    # ------------------Handling GST Table--------------------------
    if filtered_df.shape[0] == 0:
        print(f"No Matching Rows corresponding to Amount Range around {amount}")
        return

    matches = []
    perfect_match_found = False

    # -----------------------Calling Fuzzy-------------------------
    maxcs = 0
    rowcs = []
    good_match = False
    for dfindex, row in filtered_df.iterrows():
        try:
            scores = {
                "date_score": simple_fuzzy_match(
                    invoice_details["invoice_date"], row[required_keys_gst[1]], "date"
                ),
                "amount_score": simple_fuzzy_match(
                    invoice_details["invoice_amount"],
                    row[required_keys_gst[2]],
                    "amount",
                ),
                "number_score": simple_fuzzy_match(
                    invoice_details["invoice_number"],
                    row[required_keys_gst[0]],
                ),
                "gstin_score": simple_fuzzy_match(
                    invoice_details["guest_gstin"], row[required_keys_gst[4]]
                ),
            }

            total_count = 0
            total_score = 0
            if scores["amount_score"] is not None:
                total_score += scores["amount_score"]
                total_count += 1
            if scores["gstin_score"] is not None:
                total_score += scores["gstin_score"]
                total_count += 1
            if scores["number_score"] is not None:
                total_score += scores["number_score"]
                total_count += 1
            combined_score = round((total_score / total_count), 2)
            maxcs = max(maxcs, combined_score)
            if maxcs == combined_score:
                # maxds = scores["date_score"]
                rowcs = row.tolist()
        except (ValueError, TypeError) as e:
            if str(e) == "Bad Date":
                print(
                    f"Error Fuzzy Matching for \n Invoice Table = {invoice_details} with \n Booking Row = {row} \n with Error {e} \n"
                )
                return
            else:
                print(
                    f"Error Fuzzy Matching for \n Invoice Table = {invoice_details} with \n Booking Row = {row} \n with Error {e} \n"
                )

        # -------------------------Handling Scores------------------------------
        scores["combined_score"] = combined_score
        if combined_score >= 66:
            rowlist = row.tolist()
            gsttable = rowlist[5]
            rowop = retrieve_row(gsttable, df_2b, dfindex)
            # print(f"Row is {rowop} ")

            # print(
            #     f"Scores => Date: {scores['date_score']}, Amount: {scores['amount_score']}, Number: {scores['number_score']}, GSTIN: {scores['gstin_score']}"
            # )
            # print(
            #     f"Combined Score for GSTIN {row[required_keys_gst[4]]}, Invoice Number: {row[required_keys_gst[0]]}: {combined_score} for Index {invindex}"
            # )
            good_match = True
            if combined_score >= 99:
                perfect_match_found = True
                matches.clear()
                irndoc = fetch_irn_match(rowop, irn_collection)
                # print(irndoc)
                matches.append(
                    {
                        "GST_TABLE": gsttable,
                        # "RESPECTIVE_GST_DATA": row.to_dict(),
                        "RESPECTIVE_2B_DATA": rowop.to_dict(),
                        "RESPECTIVE_IRN_DATA": irndoc,
                        "SCORES": scores,
                        # "Perfect Match Status": perfect_match_found,
                    }
                )
                break

            if len(matches) < 3:
                irndoc = fetch_irn_match(rowop, irn_collection)
                matches.append(
                    {
                        "GST_TABLE": gsttable,
                        # "RESPECTIVE_GST_DATA": row.to_dict(),
                        "RESPECTIVE_2B_DATA": rowop.to_dict(),
                        "RESPECTIVE_IRN_DATA": irndoc,
                        "SCORES": scores,
                        # "Perfect Match Status": perfect_match_found,
                    }
                )
            else:
                for index in range(0, 3):
                    score = matches[index]["SCORES"]
                    if combined_score >= score["combined_score"]:
                        del matches[index]
                        irndoc = fetch_irn_match(rowop, irn_collection)
                        matches.append(
                            {
                                "GST_TABLE": gsttable,
                                # "RESPECTIVE_GST_DATA": row.to_dict(),
                                "RESPECTIVE_2B_DATA": rowop.to_dict(),
                                "RESPECTIVE_IRN_DATA": irndoc,
                                "SCORES": scores,
                                # "Perfect Match Status": perfect_match_found,
                            }
                        )
                        break

    # ------------------------Exits For Loop------------------------
    #     # --------POPULATE MONGO----------
    #     populate_mongo(
    #         rowcs,
    #         good_match,
    #         good_list,
    #         maxcs,
    #         invoice_details,
    #         perfect_match_found,
    #         ID,
    #         matches,
    #     )

    # # ------------------------------------------
    # # --------------------POPULATE MONGO-------------------------
    # def populate_mongo(
    #     rowcs,
    #     good_match,
    #     good_list,
    #     maxcs,
    #     invoice_details,
    #     perfect_match_found,
    #     ID,
    #     matches,
    # ):
    gsttable = rowcs[5]
    rowcs_dict = {
        "invoice_number": rowcs[0],
        "invoice_date": rowcs[1],
        "invoice_amount": rowcs[2],
        "hotel_gstin": rowcs[3],
        "buyer_gstin": rowcs[4],
        "gsttable": rowcs[5],
    }
    if good_match:
        stmt = {
            "max_combined_score": maxcs,
            "invoice_details": invoice_details,
            "against": rowcs_dict,
        }
        good_list.append(stmt)

    # --------------------------------- Updating Mongo DB-----------------------------------
    if perfect_match_found:
        doc = {
            "_id": ID,
            "MAX_COMBINED_SCORE": maxcs,
            "GST_TABLE": gsttable,
            "MATCHES": matches,
            "PERFECT_MATCH_STATUS": True,
            "MATCH_STATUS": True,
            "SELECTED_GST_DATA": matches[0],
        }

        # ------------Adjust if necessary to handle cases where more than 1 perfect match---------
        update_filter_criteria = {
            "_id": ID,
        }
        try:
            output_collection.insert_one(doc)
            print(
                f"Perfect match found for invoice number {invoice_details['invoice_number']}"
            )
        except Exception:
            output_collection.update_one(update_filter_criteria, {"$set": doc})
            print(
                f"Perfect match found for invoice number {invoice_details['invoice_number']}"
            )

        return
    # ---------------------------Handling Normal Matches-------------------------
    else:
        if matches:
            matches = sorted(
                matches, key=lambda x: x["SCORES"]["combined_score"], reverse=True
            )
            for match in matches:
                # Pipeline for update

                update_filter_criteria = {
                    "_id": ID,
                }

            doc = {
                "_id": ID,
                "MAX_COMBINED_SCORE": maxcs,
                "MATCHES": matches,
                "PERFECT_MATCH_STATUS": perfect_match_found,
                "MATCH_STATUS": True,
            }

            try:
                output_collection.insert_one(doc)
            except Exception:
                output_collection.update_one(update_filter_criteria, {"$set": doc})
        else:
            print(
                f"No matches found with combined score >= 66 for invoice number {invoice_details['invoice_number']} \n "
            )
            doc = {
                "_id": ID,
                "MAX_COMBINED_SCORE": maxcs,
                "MATCH_STATUS": False,
            }
            update_filter_criteria = {
                "_id": ID,
                "MAX_COMBINED_SCORE": {"$lt": maxcs},
                "MATCH_STATUS": False,
            }
            try:
                output_collection.insert_one(doc)
            except Exception:
                output_collection.update_one(update_filter_criteria, {"$set": doc})


# --------------------MultiThread/SingleThread---------------------------------
def process_booking_documents():
    query = {
        "$and": [
            # {"booking_type": "HOTEL"},
            {"invoice": {"$not": {"$eq": None}}},
            # {"MATCH_STATUS": True},
            # {"match": {"$exists": True}},
        ]
    }
    # query = {"_id": ObjectId("66b228ba79a3154f3f1cef5c")}
    booking_documents = list(booking_collection.find(query).sort("_id", 1))
    print(f"Booking documents : {len(booking_documents)}")

    df_2b = fetch_documents_by_gstin_and_load_to_dataframe(MMT_PAN_LIST, "2b")

    required_keys_gst_2b = ["inum", "dt", "val", "ctin", "gstin"]
    required_keys_gst = [
        "invoice_number",
        "invoice_date",
        "invoice_value",
        "supplier_gstin",
        "buyer_gstin",
    ]
    select2b = pd.DataFrame(df_2b, columns=required_keys_gst_2b)
    select2b = select2b.rename(
        columns={
            twob: sixa for twob, sixa in zip(required_keys_gst_2b, required_keys_gst)
        }
    )
    select2b[required_keys_gst[2]] = select2b[required_keys_gst[2]].apply(
        lambda x: convert_to_float(x)
    )
    select2b["gsttable"] = "2b"
    dfmain = pd.concat([select2b], ignore_index=True)

    print(f"Combined dataframe shape: {dfmain.shape}")
    print(f"Combined dataframe columns: {dfmain.keys()}")

    global invoicecount
    invoicecount = 0
    good_list = []

    if booking_documents:
        print(
            f"Extracted invoice data and there are {len(booking_documents)} invoices."
        )

    start_time = time.time()

    # Define the thread pool executor
    with ThreadPoolExecutor(max_workers=100) as executor:
        print("Enteering thread pool")
        # Submit all documents for processing
        futures = [
            executor.submit(process_single_document, doc, dfmain, idx, good_list, df_2b)
            for idx, doc in enumerate(booking_documents)
        ]

        # Ensure all threads complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Exception occurred: {e}")

    print(f"Total Number of Invoices Handled = {invoicecount}")
    print(f"Total Number of Good Matches = {len(good_list)}")
    print(f"Total Number of Parsed Invoices = {len(booking_documents)}")

    end_time = time.time()
    duration = end_time - start_time
    print(f"Total Duration for Fuzzy Matching = {duration}")


if __name__ == "__main__":
    # df = fetch_documents_by_gstin_and_load_to_dataframe(PAN_LIST)
    # booking_documents = list(output_collection.find())
    process_booking_documents()
