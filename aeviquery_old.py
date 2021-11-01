#! /usr/bin/env python

import os
import re
import boto3
import datetime as dt
import pprint, json, sys
import argparse
from botocore.exceptions import ClientError

# CONSTANTS
CLIENT = boto3.resource('dynamodb', region_name="eu-west-1")
TODAY = dt.datetime.fromisoformat(f"{dt.date.today()} 11:00:00").timestamp()
STATUSES = [
    "FAILED", 
    "NEW", 
    "DUPLICATE", 
    "ALREADY_CANCELLED", 
    "PROCESSING", 
    "SUCCESS"
]
ATTRIBUTES = {
    "id": "S",  # String
    "transaction_request_filename": "S",  # String
    "errorcode": "S",  # String
    "version": "N",  # Number
    "transactionreference": "S",  # String, ([0-9]{2}-){2}[0-9]{6}
    "errormessage": "S",  # String
    "created_at": "N",  # Number
    "expiration_month": "S",  # String
    "related_business_transaction_id": "S",  # String
    "status": "S",  # String
    "timestamp": "N",  # Number
    "host_merchant_id": "S",  # String
    "expected_error_code": "S",  # String
    "business_transaction_id": "S",  # String
    "st_request": "S",   # String, JSON [sitereference, dccoffered, errormessage, 
                    # accounttypedescription, paymenttypedescription, maskedpan, 
                    # requesttypedescription]
    "requestreference": "S",  # String, A[a-z0-9]{8}
    "guid": "S",  # String
    "transaction_request_id": "S",  # String
    "key_business_transaction_id": "S",  # String
    "key_guid": "S",  # String
    "host_device_id": "S",  # String
    "send_attempts": "N"  # Number
}

def queryStatus(statuses) -> dict:
    """
    Query the AEVI Transaction table on the provided status values (all by default).
    Args:
        statuses: list[str] = list of statuses to query, from 
            ["FAILED", "NEW", "DUPLICATE", "ALREADY_CANCELLED", "PROCESSING", "SUCCESS"]
    """
    results = {status: list() for status in statuses}
    for status, arr in results.items():
        if status not in STATUSES:
            raise Exception(f"Invalid status provided: {status}")
        print(f"Querying {status} transactions...", flush=True, end="")
        try:
            paginator = CLIENT.get_paginator("query")
            pageIterator = paginator.paginate(
                TableName="prod-aevi-Transaction",
                IndexName="status",
                ExpressionAttributeNames={
                    "#status": "status",
                },
                ExpressionAttributeValues={
                    ":x": status.strip()
                },
                KeyConditionExpression="#status = :x",
            )
            for page in pageIterator:
                arr.extend(page["Items"])
        except ClientError:
            print("\nNot logged in to AWSCLI!")
            sys.exit(1)
        print(f" found {len(arr)}")
    return results

def getResultsFromFile():
    with open("./results.json", "r") as file:
        results = json.load(file)
    return results

def clearResults():
    if os.path.exists("./results.json"):
        os.remove("./results.json")

def filterResults(results, filterDict):
    # prepare the filter dict
    for k, v in filterDict.items(): 
        if k == "between":  # v would be a tuple of 2 isoformat dates
            start = int(dt.datetime.fromisoformat(v["start"]+" 00:00:00").timestamp())
            end = int(dt.datetime.fromisoformat(v["end"]+" 23:59:59").timestamp())
            filterDict[k] = {"start": start, "end": end}
        elif not re.search("^st_", k):
            # Format the filter to match the AWS format
            filterDict[k] = {ATTRIBUTES[k]: str(v).strip()}
    # Do the filtering and return the filtered results
    filtered = []
    for status, arr in results.items():
        for r in arr:
            match = True  # flag to decide whether to append result or not
            for k, v in filterDict.items():
                if k == "between":  # if a date range was specified...
                    if (int(r["timestamp"]["N"]) < v["start"] or 
                            int(r["timestamp"]["N"]) > v["end"]):  # ...and the record isn't in it...
                        match = False
                        break  # ...break to next record.
                elif k not in r.keys():  # if the field isn't in the record...
                    match = False
                    break # ...break to next record.
                elif re.search("^st_", k):  # if k is an st_field...
                    if r[k] not in v:  # ...and the attribute doesn't match...
                        match = False  # ...break to next record.
                        break
                elif r[k][ATTRIBUTES[k]] not in v[ATTRIBUTES[k]]:  # if the attribute doesn't match...
                    match = False
                    break # ...break to next record.
            if match:
                filtered.append(r)
        results[status] = filtered
        filtered = []
    return results

def parseStRequest(results):
    """Expand the st_request into the main dict, each field beginning with "st_"."""
    for arr in results.values():
        for r in arr:
            if r["st_request"]["S"] != {}:
                streq = json.loads(r["st_request"]["S"])
                for field, value in streq.items():
                    r[f"st_{field}"] = value
                del r["st_request"]

def writeResultsJSON(results):
    path = "./results.json"
    with open(path, "w") as file:
        json.dump(results, file)
    print(f"Wrote results to {path}")

def main():
    parser = argparse.ArgumentParser(
        prog="aevi_query", 
        description="""Queries the DynamoDB prod-Aevi-Transactions table. 
        The status(es) to query on can be supplied, defaulting to just FAILED.
        The query is cached in the directory where the script is run as results.pkl.
        Further queries will use these cached results to save AWS allocation.
        You must be logged in to AWSCLI as Tech-Admin role, and be running in an environment with boto3 installed to run a query."""
        )
    parser.add_argument(
        "-f", "--filter", 
        metavar="FILTER", 
        type=str, 
        help="""The filter(s) to filter by.
        Format: '{"attributeName":"attributeValue"}'.
        Use "between":{"start":"YYYY-MM-DD", "end":"YYYY-MM-DD"} to supply a date range.
        The attributes inside the "st_request" attribute can be used to filter by prefixing them with st_ e.g. st_sitereference.
        If a filter is not supplied, the script will not output the results in the terminal.
        """
        )
    parser.add_argument(
        "-r", "--refresh", 
        action="store_true",
        help="""Remove the cached query and run another. Queries just FAILED unless -s specifies a different combination.""",
        )
    parser.add_argument(
        "-s", "--status", 
        metavar="STATUS", 
        type=str,
        help="""A comma-delimited string of statuses you wish to query on. Only used if --refresh is used."""
        )
    parser.add_argument(
        "-c", "--clear", 
        action="store_true",
        help="""Clear the cached query."""
        )
    args = parser.parse_args()
    if args.status and not args.refresh:
        print("A status supplied without -r, --refresh does nothing")
        sys.exit(0)
    if args.clear:
        clearResults()
        sys.exit(0)
    if args.refresh or not os.path.exists("./results.json"):
        if args.status:
            statusList = args.status.split(',')
        else:
            statusList = ["FAILED"]
        if input(f"This will query DynamoDB for {statusList} transactions. Continue? (y/N) ") == "y":
            clearResults()
            results = queryStatus(statusList)
            writeResultsJSON(results)
        else:
            sys.exit(0)
    else:
        results = getResultsFromFile()
    if args.filter:
        filterDict = json.loads(args.filter)
        for f in filterDict.keys():  # only parse st_request if a field is specified that needs it
            if "st_" in f:
                parseStRequest(results)
                break
        results = filterResults(results, filterDict=filterDict)
        for s, r in results.items():
            pprint.pprint(r)

if __name__=="__main__":
    main()