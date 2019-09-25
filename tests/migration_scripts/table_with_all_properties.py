#!/usr/bin/python

from migrator.dynamodb_migrator import Migrator
from uuid import uuid4


table_name = str(uuid4())
migrator = Migrator()


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'hash_key',
        'AttributeType': 'N'
    }, {
        'AttributeName': 'range_key',
        'AttributeType': 'S'}],
    TableName=table_name,
    KeySchema=[{
        'AttributeName': 'hash_key',
        'KeyType': 'HASH'
    }, {
        'AttributeName': 'range_key',
        'KeyType': 'RANGE'}],
    ProvisionedThroughput={
        'ReadCapacityUnits': 3,
        'WriteCapacityUnits': 2
    })
def v1(created_table):
    assert created_table['TableName'] == table_name
    assert created_table['TableStatus'] == 'ACTIVE'
