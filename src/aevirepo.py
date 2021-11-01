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

    def filterCache(self, filterDict):
        if type(filterDict) != dict:
            raise TypeError()
        if len(self._cache) == 0:
            raise EmptyCacheError()
        res = []
        for item in self._cache:
            st_request = json.loads(item["st_request"])
            match = True
            for k, v in filterDict.items():
                if (str(v) not in str(item.get(k, None)) 
                and str(v) not in str(st_request.get(k, None))):
                    match = False
                    break
            if match:
                res.append(item)
        return res

    def runIdQuery(self, id):
        kwargs = {
            "ExpressionAttributeNames": {
                "#id": "_id"
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
        if filterString: # TODO: this needs to parse st_request like filterCache does!
            expressions = []
            strings = filterString.split(',')
            for s in strings:
                ex = parseFilterString(s.strip())
                expressions.append(ex)
            filterExpression = reduce(lambda a, b: a&b, expressions)
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

    
def parseFilterString(string):  
    numericAttrs = ["timestamp", "version", "created_at"]
    if ' = ' in string:
        split = string.split(' = ')
        if split[0] in numericAttrs:
            split[1] = int(split[1])
        return Attr(split[0]).eq(split[1])
    if " contains " in string:
        split = string.split(' contains ')
        return Attr(split[0]).contains(split[1])
    if "between" in string:
        dtStr = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
        expr = re.match(f"between ({dtStr}) and ({dtStr}|now)", string)
        try:
            start, end = expr.group(1), expr.group(2)
            startDate, endDate = dt.fromisoformat(start), dt.fromisoformat(end) if end != "now" else dt.now()
        except Exception as e:
            raise 
        return Key("timestamp").between(int(startDate.timestamp()), int(endDate.timestamp()))
