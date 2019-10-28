#!/usr/bin/python

from migrator.dynamodb_migrator import Migrator


migrator = Migrator()


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'hash_key',
        'AttributeType': 'N'
    }],
    TableName='multiple_create_table_1',
    KeySchema=[{
        'AttributeName': 'hash_key',
        'KeyType': 'HASH'
    }],
    BillingMode='PAY_PER_REQUEST')
def create_table(created_table):
    pass


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'hash_key',
        'AttributeType': 'N'
    }],
    TableName='multiple_create_table_2',
    KeySchema=[{
        'AttributeName': 'hash_key',
        'KeyType': 'HASH'
    }],
    BillingMode='PAY_PER_REQUEST')
def create_another_table(created_table):
    # There are two create-statements in this file
    # There should always only be one
    # So we should never get here
    assert False
