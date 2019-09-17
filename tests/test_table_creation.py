import boto3
from mock_wrapper import mock_aws
from time import sleep


@mock_aws
def example_table_creation(dynamodb):
    print('creating table')
    dynamodb.create_table(
        AttributeDefinitions=[{
            'AttributeName': 'somekey',
            'AttributeType': 'S'
        }],
        TableName='new_table',
        KeySchema=[{
            'AttributeName': 'somekey',
            'KeyType': 'HASH'
        }],
        BillingMode='PAY_PER_REQUEST'
    )
    status = 'CREATING'
    while status != 'ACTIVE':
        status = dynamodb.describe_table(TableName='new_table')['Table']['TableStatus']
        sleep(1)
    print('created table')
    dynamodb.delete_table(TableName='new_table')
    print('deleted table')


@mock_aws
def test_create_table_script__assert_table_is_created(dynamodb):
    from migration_scripts.create_table import dynamodb_migrator
    dynamodb_migrator.migrate()
    #
    # Assert the table is created
    tables = dynamodb.list_tables()['TableNames']
    assert len(tables) == 1
    assert tables[0] == 'first_table'
    #
    dynamodb.delete_table(TableName='first_table')
