#!/usr/bin/python

from migrator.dynamodb_migrator import Migrator
from migration_scripts.add_index.table_copy_items_v1 import table_name


migrator = Migrator(identifier="copy_items")


@migrator.version(1)
@migrator.create(AttributeDefinitions=[{'AttributeName': 'customer_nr', 'AttributeType': 'S'},
                                       {'AttributeName': 'last_name', 'AttributeType': 'S'}],
                 TableName=table_name,
                 KeySchema=[{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                            {'AttributeName': 'last_name', 'KeyType': 'RANGE'}],
                 BillingMode='PAY_PER_REQUEST')
def v1(created_table):
    pass


@migrator.version(2)
@migrator.add_indexes(AttributeDefinitions=[{'AttributeName': 'postcode', 'AttributeType': 'S'}],
                      LocalSecondaryIndexes=[{'IndexName': 'postcode_index',
                                              'KeySchema': [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                                            {'AttributeName': 'postcode', 'KeyType': 'RANGE'}],
                                              'Projection': {'ProjectionType': 'ALL'}}])
def v2(created_table):
    pass
