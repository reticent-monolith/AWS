"""Rewriting the aevi_query script using TDD, pytest and moto."""
from decimal import Decimal
import pytest
import uuid
import src.aevirepo as aevi
import localstack_client.session as boto3
from moto import mock_dynamodb2
from dotenv import load_dotenv
import os
load_dotenv()

FAILED = [
    {
        "_id": "failed1",
        "status": "FAILED",
        "name": f"Failed Item",
        "transaction_filename_id": "file1",
        "st_request": "{\"sitereference\":\"site1\"}",
        "timestamp": Decimal(1633849200)
    },
    {
        "_id": "failed2",
        "status": "FAILED",
        "name": f"Failed Item",
        "transaction_filename_id": "file1",
        "st_request": "{\"sitereference\":\"site1\"}",
        "timestamp": Decimal(1633879800)
    },
    {
        "_id": "failed3",
        "status": "FAILED",
        "name": f"Failed Item",
        "transaction_filename_id": "file1",
        "st_request": "{\"sitereference\":\"site2\"}",
        "timestamp": Decimal(1634598734)
    },
    {
        "_id": "failed4",
        "status": "FAILED",
        "name": f"Failed Item",
        "transaction_filename_id": "file2",
        "st_request": "{\"sitereference\":\"site3\"}",
        "timestamp": Decimal(1634389597)
    },
    {
        "_id": "failed5",
        "status": "FAILED",
        "name": f"Failed Item",
        "transaction_filename_id": "file2",
        "st_request": "{\"sitereference\":\"site3\"}",
        "timestamp": Decimal(1634998745)
    },
]

SUCCESS = [
    {
        "_id": "success1",
        "status": "SUCCESS",
        "name": f"Success Item",
        "transaction_filename_id": "file1",
        "st_request": "{\"sitereference\":\"test_anothersite12345\"}",
        "timestamp": Decimal(1634540400)
    }
]

RECORDS = FAILED + SUCCESS

@pytest.fixture(scope="module")
def repo():
    with mock_dynamodb2():
        LOCAL = os.environ.get("AEVI_LOCAL", "False").lower() in ("true", 't', '1')
        print(LOCAL)
        repo = aevi.AeviRepo(local=LOCAL)
        if repo.isLocal:
            cl = boto3.client('dynamodb', region_name="eu-west-1")
            if "prod-aevi-Transaction" in cl.list_tables()['TableNames']:  # Check if the table exists because of localstack not triggering moto, and delete the table if it does
                cl.delete_table(TableName="prod-aevi-Transaction")
        # create dynamo table with moto
        table = repo._conn.create_table(
            TableName='prod-aevi-Transaction',
            KeySchema=[
                {'AttributeName': '_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': '_id', 'AttributeType': 'S'},
                {'AttributeName': 'status', 'AttributeType': 'S'},
                {'AttributeName': 'transaction_filename_id', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'status',
                    'KeySchema': [
                        {
                            'AttributeName': 'status',
                            'KeyType': 'HASH'
                        },
                    ],
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 1,
                        'WriteCapacityUnits': 1
                    },
                    'Projection': {
                        'ProjectionType': 'ALL',
                    },
                },
                {
                    'IndexName': 'transaction_filename_id',
                    'KeySchema': [
                        {
                            'AttributeName': 'transaction_filename_id',
                            'KeyType': 'HASH'
                        },
                    ],
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 1,
                        'WriteCapacityUnits': 1
                    },
                    'Projection': {
                        'ProjectionType': 'ALL',
                    },
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        # create 1 FAILED and 1 SUCCESSFUL records
        # insert records into table
        for item in FAILED:
            table.put_item(Item=item)
        for item in SUCCESS:
            table.put_item(Item=item)
        repo.setTable("prod-aevi-Transaction")
        yield repo

def test_runStatusQuery_FAILED(repo: aevi.AeviRepo):
    # call runStatusQuery(["FAILED"])
    res = repo.runStatusQuery(["FAILED"])
    # assert that the FAILED record is returned
    assert all(res) == all(FAILED)

def test_runStatusQuery_SUCCESS(repo: aevi.AeviRepo):
    # call runStatusQuery(["FAILED"])
    res = repo.runStatusQuery(["SUCCESS"])
    # assert that the FAILED record is returned
    assert all(res) == all(SUCCESS)

def test_setAndGetCache(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    assert all(repo.getCache()) == all(FAILED)

def test_filterCache_noCache(repo: aevi.AeviRepo):
    with pytest.raises(aevi.EmptyCacheError) as ece:
        repo.setCache([])
        repo.filterCache({})
    assert ece.type == aevi.EmptyCacheError

def test_filterCache_withCache(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "sitereference": "test_site12345"
    }
    res = repo.filterCache(filterDict)
    assert all(res) == all([i for i in FAILED if filterDict["sitereference"] in i["st_request"]])

def test_filterCache_nonString(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "timestamp": "1634540400"
    }
    res = repo.filterCache(filterDict)
    assert all(res) == all([i for i in FAILED if i["timestamp"] == Decimal(filterDict["timestamp"])])

def test_multipleFilterAttributes(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "timestamp": "1634540400",
        "sitereference": "test_site12345"
    }
    exp = [i for i in FAILED if i["timestamp"] == Decimal(filterDict["timestamp"]) and filterDict["sitereference"] in i["st_request"]]
    res = repo.filterCache(filterDict)
    assert all(res) == all(exp)

def test_multipleStatuses(repo: aevi.AeviRepo):
    statuses = ["FAILED", "SUCCESS"]
    res = repo.runStatusQuery(statuses)
    assert all(res) == all([i for i in RECORDS if i["status"] in statuses])

def test_emptyFilter(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {}
    res = repo.filterCache(filterDict)
    assert all(res) == all(FAILED)

def test_badFilter(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "derp": "herp"
    }
    res = repo.filterCache(filterDict)
    assert res == []

def test_filterNotDict(repo: aevi.AeviRepo):
    with pytest.raises(TypeError) as te:
        repo.filterCache([])
    assert te.type == TypeError

def test_runFilteredStatusQuery_stringEquals(repo: aevi.AeviRepo):
    filterString = "name = Failed Item"
    expected = FAILED
    expected.insert(1, "PAGE_END")
    expected.insert(3, "PAGE_END")
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_runFilteredStatusQuery_betweenDates(repo: aevi.AeviRepo):
    filterString = "between 2021-10-10 00:00:00 and 2021-10-15 00:00:00"
    expected = [FAILED[0], "PAGE_END"]
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_runFilteredStatusQuery_stringContains(repo: aevi.AeviRepo):
    filterString = "name contains Item"
    expected = FAILED
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_runFilteredStatusQuery_multiple(repo: aevi.AeviRepo):
    filterString = "name contains Item, between 2021-10-10 00:00:00 and 2021-10-15 00:00:00"
    expected = [FAILED[0], "PAGE_END"]
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_queryId(repo: aevi.AeviRepo):
    id = "failed1"
    expected = [item for item in RECORDS if item["_id"] == id]  # Is this a good way to desribe the expected?
    result = repo.runIdQuery(id)
    assert all(result) == all(expected)

def test_queryFilename(repo: aevi.AeviRepo):
    filename = "file1"
    expected = [item for item in RECORDS if item["transaction_filename_id"] == filename]
    result = repo.runFilenameQuery(filename)
    assert all(result) == all(expected)

def test_runFilteredQuery_stRequestAttr(repo: aevi.AeviRepo):  
    filterString = "sitereference contains another"
    expected = [i for i in FAILED if i != "PAGE_END" and "another" in i["st_request"]]
    print(expected)
    print(expected)
    result = repo.runFilteredStatusQuery(["FAILED"], filterString)
    assert all(result) == all(expected)

