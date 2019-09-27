#!/usr/bin/python

import boto3
from migrator.dynamodb_migrator import Migrator
from time import sleep
from uuid import uuid4


dynamodb = boto3.client('dynamodb')
table_name = str(uuid4())
migrator = Migrator(identifier='make examples/simple_table.py')


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'hash_key',
        'AttributeType': 'N'
    }],
    TableName=table_name,
    KeySchema=[{
        'AttributeName': 'hash_key',
        'KeyType': 'HASH'
    }],
    BillingMode='PAY_PER_REQUEST')
def v1(created_table):
    assert created_table['TableName'] == table_name
    assert created_table['TableStatus'] == 'ACTIVE'
    print("===================")
    print("Script has finished")
    print("We can now use the created table as appropriate")
    print("As this is only an example, we'll delete the tables again, so that we're not incurring unexpected costs")
    print("This might take a while...")
    delete_table(table_name)
    delete_table('dynamodb_migrator_metadata')


def delete_table(name):
    try:
        dynamodb.delete_table(TableName=name)
        while True:
            dynamodb.describe_table(TableName=name)
            sleep(1)
    except dynamodb.exceptions.ResourceNotFoundException:
        # Table might not exist (anymore)
        pass


migrator.migrate()
