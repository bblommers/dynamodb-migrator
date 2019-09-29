#!/usr/bin/python

from migrator.dynamodb_migrator import Migrator
from uuid import uuid4


migrator = Migrator()


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'hash_key',
        'AttributeType': 'N'
    }],
    TableName=str(uuid4()),
    KeySchema=[{
        'AttributeName': 'hash_key',
        'KeyType': 'HASH'
    }],
    BillingMode='PAY_PER_REQUEST')
def create_table(created_table):
    # There are two create-statements in this file
    # There should always only be one
    # The table should never be created
    # And we should never get here
    assert False


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'hash_key',
        'AttributeType': 'N'
    }],
    TableName=str(uuid4()),
    KeySchema=[{
        'AttributeName': 'hash_key',
        'KeyType': 'HASH'
    }],
    BillingMode='PAY_PER_REQUEST')
def create_another_table(created_table):
    # There are two create-statements in this file
    # There should always only be one
    # The table should never be created
    # And we should never get here
    assert False
