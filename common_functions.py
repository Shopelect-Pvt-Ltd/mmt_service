from datetime import datetime
from fuzzywuzzy import fuzz
import pandas as pd
import re
import json


# ----------------Format GST 2b-flat date column----------------------
def format_date_column_2b(df, date_column_name, date_format="%d/%m/%Y"):
    if date_column_name in df.columns:
        df[date_column_name] = pd.to_datetime(df[date_column_name], errors="coerce")
        df[date_column_name] = df[date_column_name].dt.strftime(date_format)
    else:
        print(f"Column '{date_column_name}' not found in the DataFrame")
    return df


# -----------------Clean Amount for invoice details-------------------
def clean_amount(amount_str):
    amount_str = str(amount_str)
    pattern = r"[^0-9.]"
    cleaned_amount = re.sub(pattern, "", amount_str)
    return cleaned_amount


# -------------------Convert Value to float if string Basically a better Clean Amount-----------------------
def convert_to_float(value):
    if isinstance(value, str):
        return float(value.replace(",", ""))
    else:
        return float(value)


# ---------------------Check if Empty------------------------------
def is_empty(value):
    return value is None or value == "" or value == "N/A" or pd.isna(value)


# -----------------Format GST 6a date column---------------------------
def format_date_column_6a(df, date_column_name, date_format="%d/%m/%Y"):
    if date_column_name in df.columns:
        df[date_column_name] = df[date_column_name].str.replace("-", "/")
    else:
        print(f"Column '{date_column_name}' not found in the DataFrame")
    return df


# ------------------Used to Parse Date in Fuzzy Match--------------------
def parse_date(date_str):
    date_formats = ["%d/%m/%Y", "%d/%m/%y", "%H:%M %d-%b-%Y", "%Y-%m-%d %H:%M"]
    if not date_str:
        raise ValueError(f"Time data {date_str} is an empty string")
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"time data '{date_str}' does not match any valid format")


# --------------------Retrieve Row By Index--------------------
def retrieve_row(gsttable, df_2b, dfindex):
    if gsttable == "2b":
        rowop = df_2b.iloc[dfindex]
    # if gsttable == "6a":
    #     len = df_csv.shape[0]
    #     newindex = dfindex - len
    #     rowop = df_6a.iloc[newindex]
    # else:
    #     rowop = df_csv.iloc[dfindex]
    return rowop


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


def fetch_irn_match(rowop, irn_collection):
    irn = rowop["irn"]
    # print(f"IRN of row is {irn} ")

    if not is_empty(irn):
        # print(f"------The Value of irn exists:{irn} ---------")
        irnquery = {"Irn": str(irn)}
        irndoc = list(irn_collection.find(irnquery).limit(1))
        # print(f"IRN DOC in fUnction is {irndoc}")
        if not irndoc:
            return "No Invoice present in IRN data for the IRN Number"
        irndoc = irndoc[0]
        return irndoc
        # print(f"The IRN Doc is {irndoc} ")
    else:
        irndoc = "No IRN Number Present In GST Information"
        return irndoc


def fetch_invoice_status(invoiceObj):
    try:
        invoice_status = invoiceObj["parsed_invoice"]
        invoice_status = "Invoice with Valid Link"
        return invoice_status
    except KeyError:
        try:
            invoice_status = invoiceObj["invoiceUrl"]
            invoice_status = "Invoice with Invalid Link"
            return invoice_status
        except KeyError:
            invoice_status = "Invoice with No Link"
            return invoice_status
