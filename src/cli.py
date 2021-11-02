#! /usr/bin/env python

from botocore.exceptions import ClientError
import aevirepo
import pprint
import os
import json
import readline

def displayRecord(record):
    string = ""
    for k, v in record.items():
        if k == "st_request":
            v = pprint.pformat(v)
        string += k.ljust(30)
        string += str(v)
        string += '\n'
    print(string)

def runFilteredQuery(repo, args):
    filterString = input("Filter: ")
    args.append(filterString)
    print("\n===================================")
    try:
        for item in repo.runFilteredStatusQuery(*args):
            if item not in ["PAGE_END"]:
                displayRecord(item)
            elif item == "PAGE_END":
                print("===================================")
                if input("\nContinue? ") == 'n':
                    break
    except ClientError:
        raise

def runNormalQuery(repo, args):
    print("Querying... ", end='', flush=True)
    res = repo.runStatusQuery(*args)
    print(f"Found {len(res)} records!")
    if len(res) > 0:
        repo.setCache(res)

def filterCachedQuery(repo):
    filterDict = {}
    while True:
        attr = input("\nAttribute: ")
        val = input(f"Value for {attr}: ")
        if val == "" or attr == "":
            print("Invalid field or attribute!")
            continue
        filterDict[attr] = val
        if input("Add another? (y/N)") != 'y':
            break
        pprint.pprint(filterDict)
    res = repo.filterCache(filterDict)
    print("\n===================[RESULTS]===================")
    for i, item in enumerate(res):
        displayRecord(item)
        if i != len(res)-1:
            print()
    print("=====================[END]=====================")

def loadSavedQuery(repo):
    try:
        path = input("Absolute path to file (default is ./results.json): ")
        if path == "":
            path = "./results.json"
        repo.setCache(json.load(open(path, "r")))
    except:
        raise Exception("Can't find that file")

def saveQuery(repo):
    path = input("Save as (include absolute path, default is ./results.json): ")
    if path == "":
        path = "./results.json"
    json.dump(repo.getCache(), open(path, "w"))

def showMenu(repo):
    menu = ["[R]un query", "[F]ilter cached query", "[L]oad saved query"]
    print("\n===============")
    print("AEVI QUERY v0.1")
    print("===============")
    for item in menu:
        print(item)
    if len(repo.getCache()) > 0:
        print("[S]ave current query")
    print("[Q]uit")

def run(local):
    repo = aevirepo.AeviRepo(local)
    repo.setTable('prod-aevi-Transaction')
    while True:
        showMenu(repo)
        choice = input("\nWhat do you want to do? ").lower()
        if choice == "q":
            if len(repo.getCache()) > 0:
                if input("\nThere is a cached query. Are you sure you want to quit? (y/N) ").lower() == 'y':
                    break
            break
        elif choice == "r":
            if len(repo.getCache()) > 0:
                if input("\nThere is already a cached query that can be filtered. Are you sure you want to run another? (y/N) ").lower() != 'y':
                    continue
            statuses = input("\nEnter statuses to query in comma delimited string: ")
            args = [statuses.split(',')]
            if input("Apply a filter? (y/N) ") == 'y':
                try:
                    runFilteredQuery(repo, args)
                except ClientError:
                    continue
            else:
                try:
                    runNormalQuery(repo, args)
                except ClientError:
                    continue
        elif choice == "f":
            try:
                filterCachedQuery(repo)
            except:
                continue
        elif choice == "s" and len(repo.getCache()) > 0:
            saveQuery(repo)
        elif choice == "l":
            try:
                loadSavedQuery(repo)
            except:
                continue
    print("\nBye!\n")


