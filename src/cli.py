#! /usr/bin/env python

from botocore.exceptions import ClientError
import aevirepo
import pprint
import json
import readline  # Makes taking input() not awful
from readchar import readchar as read
from os import system
from colorama import Fore, Style, init
init()

def displayRecord(record):
    string = ""
    for k, v in record.items():
        if k == "st_request":
            v = '\n' + pprint.pformat(json.loads(v))
        string += Fore.RED
        string += k.ljust(30)
        string += Style.RESET_ALL
        string += str(v)
        string += '\n'
    print(string)

def runFilteredQuery(repo, args):
    """Run a query with a filter expression used by the database itself. Currently returns one item at a time with the option to continue or quit."""
    filterString = input("Filter: ")
    if filterString == "":
        return "NO_FILTER"
    args.append(filterString)
    print("\n============================")
    try:
        for item in repo.runFilteredStatusQuery(*args):
            if item not in ["PAGE_END"]:
                displayRecord(item)
            elif item == "PAGE_END":
                print("============================")
                print("\nEnter to continue, q to quit")
                if read() == 'q':
                    break
    except ClientError:
        raise

def runQuery(repo):
    """Decides which type of query to run based on "Filter" response."""
    if len(repo.getCache()) > 0:
        print("\nThere is already a cached query that can be filtered. Are you sure you want to run another? (y/N) ")
        if read().lower() != 'y':
            raise Exception("CANCEL")
    statuses = input("\nEnter statuses to query in comma delimited string: ")
    args = [statuses.split(',')]
    try:
        if runFilteredQuery(repo, args) == "NO_FILTER":
            try:
                runNormalQuery(repo, args)
            except ClientError:
                raise
    except ClientError:
        raise
        
def runNormalQuery(repo, args):
    """Run a query without any filters. Will cache the result if not of length 0, but will not display the records on screen."""
    print("Querying... ", end='', flush=True)
    res = repo.runStatusQuery(*args)
    print(f"Found {len(res)} records!")
    if len(res) > 0:
        repo.setCache(res)

def filterCachedQuery(repo):
    """Filter the currently cached query."""
    filterDict = {}
    while True:
        attr = input("\nAttribute: ")
        val = input(f"Value for {attr}: ")
        if val == "" or attr == "":
            print("Invalid field or attribute!")
            continue
        filterDict[attr] = val
        print("Add another? (y/N)")
        if read() != 'y':
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
    """Load a saved query from the given file path."""
    try:
        path = input("Absolute path to file (default is ./results.json): ")
        if path == "":
            path = "./results.json"
        repo.setCache(json.load(open(path, "r")))
    except:
        raise Exception("Can't find that file")

def saveQuery(repo):
    """Save a query to a json file at given path."""
    path = input("Save as (include absolute path, default is ./results.json): ")
    if path == "":
        path = "./results.json"
    json.dump(repo.getCache(), open(path, "w"))

def showMenu(repo):
    """Show the main menu for the app. If there is a cached query (either from running one or loading one) the menu will show the save option."""
    menu = ["[R]un query", "[F]ilter cached query", "[L]oad saved query"]
    print("\n===============")
    print("AEVI QUERY v0.1")
    print("===============")
    for item in menu:
        print(item)
    if len(repo.getCache()) > 0:
        print("[S]ave current query")
    print("[Q]uit")
    print("\nWhat do you want to do? ")
    choice = read().lower()
    return choice

def quit(repo):
    """Close the app."""
    if len(repo.getCache()) > 0:
        print("\nThere is a cached query. Are you sure you want to quit? (y/N) ")
        if read().lower() == 'y':
            return "QUIT"
        return "STAY"
    return "QUIT"

def run(local):
    """Run the app. Called by main.py"""
    repo = aevirepo.AeviRepo(local)
    repo.setTable('prod-aevi-Transaction')
    while True:
        choice = showMenu(repo)
        if choice == "q":
            if quit(repo) != "STAY":
                break
        elif choice == "r":
            try:
                runQuery(repo)
            except Exception:
                continue
            finally:
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


