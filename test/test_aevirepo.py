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

RECORDS = [
    {
        'business_transaction_id': '11710742480',
        'created_at': 1627308098,
        'errorcode': '30000',
        'errormessage': 'Invalid field',
        'expected_error_code': '0',
        'expiration_month': 'TRUNCATED',
        'guid': 'FDB7DC52-DBC7-4FE9-B2A3-BE8328A6D744',
        'host_device_id': 'T0000561',
        'host_merchant_id': '000104960034730',
        'id': '953ee84c12fa462690497d4444c79131790d203d93717a89efcc3e9070b6a1cc',
        'key_business_transaction_id': '000104960034730AuthorisationPurchase11710742480',
        'key_guid': '000104960034730AuthorisationPurchaseFDB7DC52-DBC7-4FE9-B2A3-BE8328A6D744',
        'requestreference': 'Ab523q53f',
        'send_attempts': 1,
        'st_request': '{"requesttypedescription": "AUTH", "accounttypedescription": "POS", "acquirertransactionid": "12315712554", "acquirertransactionreference": "6453135102", "acquirerresponsecode": "00", "acquirerguid": "B55F2503-6A5D-401E-B312-23E69EB6D577", "retrievalreferencenumber": "122314321402", "terminalid": "T0006205", "authcode": "127705", "stan": "7997", "deviceprocessingmode": "Online", "mainamount": "18.25", "currencyiso3a": "EUR", "errorcode": "0", "transactionstartedtimestamp": "2021-08-11 12:08:20", "maskedpan": "400000******1000", "paymenttypedescription": "VISA", "cardentrymode": "ChipContactless", "cardholderverificationmethod": "None", "issuer": "", "stationname": "merchant name", "stationbusinessid": "merchant name", "stationid": "6696231975", "stationoriginalid": "97180", "stationstreet": "", "stationpremise": "", "stationtown": "", "stationpostcode": "", "stationcountryiso2a": "", "deviceid": "9571510164", "deviceconfigurationversion": "", "devicesoftwareversion": "0401.132095", "merchantcategorycode": "5411", "deviceattended": "", "devicecategory": "EFTPOSTerminal", "devicetype": "IndoorPaymentTerminal", "devicemodel": "CastlesHW_V3C", "devicevendor": "", "customfield1": "aevi-listener-5e77f14850a38a6a404c17c128f6a54208210078", "dcctype": "DCC", "dccoffered": "2", "dccprovider": "FEXCO", "dcccurrencyiso3a": "EUR", "dccmainamount": "18.25", "sitereference": "siteref45678"}',
        'status': 'FAILED',
        'timestamp': 1635725027,
        'transaction_request_filename': 'b1551dc2-f3e2-4de7-8075-266805b65778',
        'transaction_request_id': 'b1551dc2-f3e2-4de7-8075-266805b65778:0',
        'version': 7
    },
    {
        'business_transaction_id': '11710742231',
        'created_at': 1627306534,
        'errorcode': '30000',
        'errormessage': 'Invalid field',
        'expected_error_code': '0',
        'expiration_month': 'TRUNCATED',
        'guid': 'FDB7DC52-DBC7-4FE9-B2A3-BE8328A6D744',
        'host_device_id': 'T00007652',
        'host_merchant_id': '000104960034897',
        'id': '953ee84c12fa462690497d4345c79131790d203d93717a89efcc3e9070b6a1cc',
        'key_business_transaction_id': '000104960034730AuthorisationPurchase11710742480',
        'key_guid': '000104960034730AuthorisationPurchaseFDB7DC52-DBC7-4FE9-B2A3-BE8328A6D744',
        'requestreference': 'Ab823q53f',
        'send_attempts': 1,
        'st_request': '{"sitereference":"test_site12345"}',
        'status': 'FAILED',
        'timestamp': 1635721233,
        'transaction_request_filename': 'b15513c2-f3e2-4de7-8075-266805b65778',
        'transaction_request_id': 'b1551dc2-f3e2-4de7-8075-266805b65778:0',
        'version': 7
    },
    {
        'business_transaction_id': '11710742678',
        'created_at': 1627302308,
        'errorcode': '30000',
        'errormessage': 'Invalid field',
        'expected_error_code': '0',
        'expiration_month': 'TRUNCATED',
        'guid': 'FDB7DC52-DBC7-4FE9-B2A3-BE8328A6D744',
        'host_device_id': 'T0000561',
        'host_merchant_id': '000104960034212',
        'id': '953ee84c12fa462690497d4444c79178590d203d93717a89efcc3e9070b6a1cc',
        'key_business_transaction_id': '000104960034730AuthorisationPurchase11710742480',
        'key_guid': '000104960034730AuthorisationPurchaseFDB7DC52-DBC7-4FE9-B2A3-BE8328A6D744',
        'requestreference': 'Ab533q53f',
        'send_attempts': 1,
        'st_request': '{}',
        'status': 'FAILED',
        'timestamp': 1633824100,
        'transaction_request_filename': 'b1551dc2-f3e2-4de7-8075-266805b65778',
        'transaction_request_id': 'b1551dc2-f3e2-4de7-8075-266805b65778:0',
        'version': 7
    },
]

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
                {'AttributeName': 'id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
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
        for item in RECORDS:
            table.put_item(Item=item)
        repo.setTable("prod-aevi-Transaction")
        yield repo

def test_runStatusQuery_FAILED(repo: aevi.AeviRepo):
    # call runStatusQuery(["FAILED"])
    res = repo.runStatusQuery(["FAILED"])
    # assert that the FAILED record is returned
    assert all(res) == all([r for r in RECORDS if r["errormessage"] == "Invalid field"])

def test_runStatusQuery_SUCCESS(repo: aevi.AeviRepo):
    # call runStatusQuery(["FAILED"])
    res = repo.runStatusQuery(["SUCCESS"])
    # assert that the FAILED record is returned
    assert all(res) == all([r for r in RECORDS if r['status'] == "SUCCESS"])

def test_setAndGetCache(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    assert all(repo.getCache()) == all([r for r in RECORDS if r["errormessage"] == "Invalid field"])

def test_filterCache_noCache(repo: aevi.AeviRepo):
    with pytest.raises(aevi.EmptyCacheError) as ece:
        repo.setCache([])
        repo.filterCache({})
    assert ece.type == aevi.EmptyCacheError

def test_filterCache_withCache(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterString = "sitereference is test_site12345"
    res = repo.filterCache(filterString)
    assert all(res) == all([i for i in RECORDS if "test_site12345" in i["st_request"] and i['status'] == "FAILED"])

def test_filterCache_nonString(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "timestamp": "1634540400"
    }
    res = repo.filterCache(filterDict)
    assert all(res) == all([i for i in RECORDS if i["timestamp"] == Decimal(filterDict["timestamp"]) and i["errormessage"] == "Invalid field"])

def test_multipleFilterAttributes(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "timestamp": "1634540400",
        "sitereference": "test_site12345"
    }
    exp = [i for i in RECORDS if i["timestamp"] == Decimal(filterDict["timestamp"]) and filterDict["sitereference"] in i["st_request"] and i["errormessage"] == "Invalid field"]
    res = repo.filterCache(filterDict)
    assert all(res) == all(exp)

def test_multipleStatuses(repo: aevi.AeviRepo):
    statuses = ["FAILED", "SUCCESS"]
    res = repo.runStatusQuery(statuses)
    assert all(res) == all([i for i in RECORDS if i["status"] in statuses])

def test_emptyFilter(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterString = ""
    res = repo.filterCache(filterString)
    assert all(res) == all([r for r in RECORDS if r["errormessage"] == "Invalid field"])

def test_badFilter(repo: aevi.AeviRepo):
    repo.setCache(repo.runStatusQuery(["FAILED"]))
    filterDict = {
        "derp": "herp"
    }
    res = repo.filterCache(filterDict)
    assert res == []

def test_runFilteredStatusQuery_stringEquals(repo: aevi.AeviRepo):
    filterString = "errormessage is Invalid field"
    expected = [r for r in RECORDS if r["errormessage"] == "Invalid field"]
    expected.insert(1, "PAGE_END")
    expected.insert(3, "PAGE_END")
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_runFilteredStatusQuery_betweenDates(repo: aevi.AeviRepo):
    filterString = "between 2021-10-10 00:00:00 and 2021-10-15 00:00:00"
    expected = [r for r in RECORDS if 1633824000 <= r["timestamp"] <= 1634256000]
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_runFilteredStatusQuery_stringContains(repo: aevi.AeviRepo):
    filterString = "errormessage contains Invalid"
    expected = [r for r in RECORDS if "Invalid" in r['errormessage']]
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_runFilteredStatusQuery_multiple(repo: aevi.AeviRepo):
    filterString = "errormessage is Invalid field; between 2021-10-10 00:00:00 and 2021-10-15 00:00:00"
    expected = [r for r in RECORDS if r["errormessage"] == "Invalid field" and 1633824000 <= r["timestamp"] <= 1634256000]
    result = []
    for i in repo.runFilteredStatusQuery(["FAILED"], filterString):
        result.append(i)
    assert all(result) == all(expected)

def test_queryId(repo: aevi.AeviRepo):
    id = "failed1"
    expected = [item for item in RECORDS if item["id"] == id]  # Is this a good way to desribe the expected?
    result = repo.runIdQuery(id)
    assert all(result) == all(expected)

def test_queryFilename(repo: aevi.AeviRepo):
    filename = "file1"
    expected = [item for item in RECORDS if item["transaction_request_filename"] == filename]
    result = repo.runFilenameQuery(filename)
    assert all(result) == all(expected)

def test_runFilteredQuery_stRequestAttr(repo: aevi.AeviRepo):  
    filterString = "sitereference contains siteref"
    expected = [i for i in RECORDS if i["errormessage"] == "Invalid field" and i != "PAGE_END" and "siteref" in i["st_request"]]
    result = repo.runFilteredStatusQuery(["FAILED"], filterString)
    assert all(result) == all(expected)

