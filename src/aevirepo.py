from decimal import Decimal
import localstack_client.session as boto3_local  # todo: this is only for connecting to localstack
import boto3
import json
from boto3.dynamodb.conditions import Attr, Key
from functools import reduce
import re
from datetime import datetime as dt

from botocore.exceptions import ClientError

class EmptyCacheError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class AeviRepo():
    """Query the DynamoDB database. Requires the current shell to be signed in to AWS."""
    def __init__(self, local: bool):
        self.isLocal = local
        if local:
            print("Running locally")
        else:
            print("Not running locally")
        boto = boto3_local if self.isLocal else boto3
        self._conn = boto.resource("dynamodb", region_name="eu-west-1")
        self._table: boto.dynamodb.Table = None
        self._cache = []

    def setTable(self, tableName):
        """Connect to the specified table."""
        self._table = self._conn.Table(tableName)

    def runStatusQuery(self, statuses: list) -> list:
        """Returns a list of records matching the query"""
        res = []
        for status in statuses:
            for item in self._queryStatus(status):
                res.append(item)
        return res

    def runFilteredStatusQuery(self, statuses: list, filterString=None) -> dict:
        """Yields a page of results matching the query. 
        PAGE_END is yielded at the end of a page to check for continuation."""
        for status in statuses:
            for item in self._queryStatus(status, filterString):
                yield item

    def setCache(self, results):
        self._cache = results

    def getCache(self):
        return self._cache

    def filterCache(self, filterString) -> list:
        cache = self.getCache() 
        if len(cache) == 0:
            raise EmptyCacheError() # no point running this if there's nothing in the cache!
        res = []
        filters = parseFilterStringForCache(filterString)
        for item in cache: # loop through the records saved to the cache
            for f in filters: # loop through the filters supplied
                st_request = json.loads(item["st_request"]) # load st_request as a dictionary so it's easier to filter
                # check the matches
                if (f["op"] == 'is' and item.get(f["attr"], None) != f["val"] and
                        st_request.get(f["attr"], "") != f["val"]):
                    break
                if (f["op"] == 'contains' and f["val"] not in item.get(f["attr"], None) and
                        st_request.get(f["attr"], None) not in f["val"]):
                    break
                if (f["op"] == "between" and not (item["timestamp"] >= f["start"] and 
                        item["timestamp"] <= f["end"])):
                    break
                res.append(item)
        return res

    def runIdQuery(self, id):
        kwargs = {
            "ExpressionAttributeNames": {
                "#id": "id"
            },
            "ExpressionAttributeValues": {
                ":x": id.strip()
            },
            "KeyConditionExpression": "#id = :x",
        }
        res = self._table.query(**kwargs)
        return res["Items"]

    def runFilenameQuery(self, filename):
        kwargs = {
            "IndexName": "transaction_filename_id",
            "ExpressionAttributeNames": {
                "#x": "transaction_filename_id"
            },
            "ExpressionAttributeValues": {
                ":y": filename.strip()
            },
            "KeyConditionExpression": "#x = :y"
        }
        res = self._table.query(**kwargs)
        return res["Items"]
    

    # Private Methods ---------------------------------------------------------------
    def _queryStatus(self, status, filterString=None):
        """Query the current table with the specified status as index."""
        done = False
        startKey = None
        kwargs = {
            "IndexName": "status",
            "ExpressionAttributeNames": {
                "#status": "status"
            },
            "ExpressionAttributeValues": {
                ":x": status.strip()
            },
            "KeyConditionExpression": "#status = :x",
        }
        if filterString:
            expressions = []
            strings = filterString.split(';')
            for s in strings:
                ex = parseFilterStringForBoto(s.strip())
                expressions.append(ex)
            filterExpression = reduce(lambda a, b: a&b, expressions)
            print(filterExpression)
            kwargs["FilterExpression"] = filterExpression
            kwargs["Limit"] = 1
        # Paginate!
        while not done:
            if startKey:
                kwargs["ExclusiveStartKey"] = startKey
            try:
                res = self._table.query(**kwargs)
            except ClientError as ce:
                print(ce)
                raise
            startKey = res.get("LastEvaluatedKey", None)
            done = startKey is None
            items = res.get("Items", [])
            for item in items:
                for k, v in item.items():
                    if type(v) == Decimal:
                        item[k] = int(v)
                yield item
            if filterString and len(items) > 0:
                yield "PAGE_END"

    
def parseFilterStringForBoto(string) -> object: 
    numericAttrs = ["timestamp", "version", "created_at"]
    if ' is ' in string:
        split = string.split(' is ')
        split = [s.strip(" ") for s in split]
        if split[0] in numericAttrs:
            split[1] = int(split[1])
        return Attr(split[0]).eq(split[1])
    if " contains " in string:
        split = string.split(' contains ')
        return Attr(split[0]).contains(split[1])
    if "between " in string:
        dtStr = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
        expr = re.match(f"between ({dtStr}) and ({dtStr}|now)", string)
        try:
            start, end = expr.group(1), expr.group(2)
            startDate, endDate = dt.fromisoformat(start), dt.fromisoformat(end) if end != "now" else dt.now()
        except Exception:
            raise 
        return Key("timestamp").between(int(startDate.timestamp()), int(endDate.timestamp()))

def parseFilterStringForCache(string: str) -> list:  # I hate this function...
    """Returns a list of dictionaries describing different specified filters to use on the cached query."""
    numericAttrs = ["timestamp", "version", "created_at"]
    filters = []
    if ' is ' in string:
        split = string.split(' is ')
        split = [s.strip(" ") for s in split]
        if split[0] in numericAttrs:
            split[1] = int(split[1])
        filterDict = {
            "attr": split[0],
            "val": split[1],
            "op": 'is'
        }
        filters.append(filterDict)
    if " contains " in string:
        split = string.split(' contains ')
        filterDict = {
            "attr": split[0],
            "val": split[1],
            "op": 'contains'
        }
        filters.append(filterDict)
    if "between " in string:
        dtStr = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
        expr = re.match(f"between ({dtStr}) and ({dtStr}|now)", string)
        try:
            start, end = expr.group(1), expr.group(2)
            startDate, endDate = dt.fromisoformat(
                start), dt.fromisoformat(end) if end != "now" else dt.now()
        except Exception:
            raise
        filterDict = {
            "attr": "timestamp",
            "start": startDate,
            "end": endDate,
            "op": "between"
        }
        filters.append(filterDict)
    return filters
