#!/usr/bin/python

import boto3
from migrator.dynamodb_migrator import Migrator
from time import sleep


dynamodb = boto3.client('dynamodb')
iam = boto3.client('iam')
lmbda = boto3.client('lambda')
table_name = 'customers'
migrator = Migrator(identifier='make examples/customer_table.py')


def delete_table(name):
    try:
        dynamodb.delete_table(TableName=name)
        while True:
            dynamodb.describe_table(TableName=name)
            sleep(1)
    except dynamodb.exceptions.ResourceNotFoundException:
        # Table might not exist (anymore)
        pass


def delete_tables(names):
    for name in names:
        delete_table(name)


def delete_created_services():
    created_items = dynamodb.scan(TableName='dynamodb_migrator_metadata')['Items'][0]['2']['M']
    print("The following items will be deleted:")  # noqa: T001
    print(created_items)  # noqa: T001
    role_arn = created_items['roles']['SS'][0]
    role_name = role_arn[role_arn.rindex('/') + 1:]
    lmbda.delete_event_source_mapping(UUID=created_items['mappings']['SS'][0])
    lmbda.delete_function(FunctionName=created_items['functions']['SS'][0])
    iam.detach_role_policy(RoleName=role_name, PolicyArn=created_items['policies']['SS'][0])
    iam.delete_policy(PolicyArn=created_items['policies']['SS'][0])
    iam.delete_role(RoleName=role_name)
    delete_tables(['dynamodb_migrator_metadata', 'customers', 'customers_V2'])


@migrator.version(1)
@migrator.create(
    AttributeDefinitions=[{'AttributeName': 'customer_nr', 'AttributeType': 'N'},
                          {'AttributeName': 'last_name', 'AttributeType': 'S'}],
    TableName=table_name,
    KeySchema=[{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
               {'AttributeName': 'last_name', 'KeyType': 'RANGE'}],
    BillingMode='PAY_PER_REQUEST')
def v1(created_table):
    assert created_table['TableName'] == table_name
    assert created_table['TableStatus'] == 'ACTIVE'
    print("===================")  # noqa: T001
    print("Script has finished")  # noqa: T001
    print("We can now use the created table as appropriate")  # noqa: T001
    print("Several months later.. we've forgotten to add an index!")  # noqa: T001


@migrator.version(2)
@migrator.add_indexes(AttributeDefinitions=[{'AttributeName': 'postcode', 'AttributeType': 'S'}],
                      LocalSecondaryIndexes=[{'IndexName': 'postcode_index',
                                              'KeySchema': [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                                            {'AttributeName': 'postcode', 'KeyType': 'RANGE'}],
                                              'Projection': {'ProjectionType': 'ALL'}}])
def v2(updated_table):
    print("Script will now have created a new table with the necessary index added to it")  # noqa: T001
    print("All data from the old table has also been send to the new table")  # noqa: T001
    print("As this is only an example, we'll delete everything from AWS, so that we're not incurring unexpected costs")  # noqa: T001
    print("This might take a while...")  # noqa: T001
    delete_created_services()
    print("All tables/functions/roles/policies have been deleted!")  # noqa: T001
