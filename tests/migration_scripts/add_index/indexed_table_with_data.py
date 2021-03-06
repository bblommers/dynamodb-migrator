#!/usr/bin/python

from migrator.dynamodb_migrator import Migrator


table_name = 'customers'
migrator = Migrator(identifier="index_with_data")


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{
        'AttributeName': 'customer_nr',
        'AttributeType': 'S'
    }, {
        'AttributeName': 'last_name',
        'AttributeType': 'S'
    }],
    TableName=table_name,
    KeySchema=[{
        'AttributeName': 'customer_nr',
        'KeyType': 'HASH'
    }, {
        'AttributeName': 'last_name',
        'KeyType': 'RANGE'
    }],
    BillingMode='PAY_PER_REQUEST')
def v1(created_table):
    import boto3
    dynamodb = boto3.client('dynamodb')
    dynamodb.put_item()


@migrator.version(2)
@migrator.add_indexes(AttributeDefinitions=[{'AttributeName': 'postcode', 'AttributeType': 'S'}],
                      LocalSecondaryIndexes=[{
                          'IndexName': 'string',
                          'KeySchema': [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                        {'AttributeName': 'postcode', 'KeyType': 'RANGE'}],
                          'Projection': {'ProjectionType': 'ALL'}}])
def v2(created_table):
    assert created_table['TableName'] == f"{table_name}_V2"
    assert created_table['TableStatus'] == 'ACTIVE'
