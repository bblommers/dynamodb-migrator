#!/usr/bin/python

from migrator.dynamodb_migrator import Migrator


table_name = 'customers'
migrator = Migrator(identifier="table_with_provisioned_throughput")


@migrator.version(1)
@migrator.create(AttributeDefinitions=[{'AttributeName': 'customer_nr', 'AttributeType': 'S'},
                                       {'AttributeName': 'last_name', 'AttributeType': 'S'},
                                       {'AttributeName': 'postcode', 'AttributeType': 'S'}],
                 TableName=table_name,
                 KeySchema=[{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                            {'AttributeName': 'last_name', 'KeyType': 'RANGE'}],
                 LocalSecondaryIndexes=[{'IndexName': 'existing_index',
                                         'KeySchema': [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                                       {'AttributeName': 'postcode', 'KeyType': 'RANGE'}],
                                         'Projection': {'ProjectionType': 'ALL'}}],
                 BillingMode='PROVISIONED',
                 ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 2})
def v1(created_table):
    assert created_table['TableName'] == table_name
    assert created_table['TableStatus'] == 'ACTIVE'


@migrator.version(2)
@migrator.add_indexes(AttributeDefinitions=[{'AttributeName': 'password', 'AttributeType': 'S'}],
                      LocalSecondaryIndexes=[{'IndexName': 'new_index',
                                              'KeySchema': [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                                            {'AttributeName': 'password', 'KeyType': 'RANGE'}],
                                              'Projection': {'ProjectionType': 'ALL'}}])
def v2(created_table):
    assert created_table['TableName'] == f"{table_name}_V2"
    assert created_table['TableStatus'] == 'ACTIVE'
