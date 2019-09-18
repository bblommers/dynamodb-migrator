import pytest
from .mock_wrapper import mock_aws
from time import sleep


@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    # Clean up
    # We don't want to know about previous test-functions
    from migrator import dynamodb_migrator
    dynamodb_migrator._function_list.clear()


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
    from .migration_scripts.simple_table import dynamodb_migrator
    dynamodb_migrator.migrate()
    from .migration_scripts.simple_table import table_name
    #
    # Assert the table is created
    table_names = dynamodb.list_tables()['TableNames']
    assert len(table_names) >= 1
    assert table_name in table_names
    #
    dynamodb.delete_table(TableName=table_name)


@mock_aws
def test_create_table_script__assert_table_has_all_properties(dynamodb):
    from .migration_scripts.table_with_all_properties import dynamodb_migrator
    dynamodb_migrator.migrate()
    from .migration_scripts.table_with_all_properties import table_name
    #
    # Assert the table has the right properties
    table = dynamodb.describe_table(TableName=table_name)['Table']
    assert table['TableStatus'] == 'ACTIVE'
    assert table['AttributeDefinitions'] == [{'AttributeName': 'hash_key', 'AttributeType': 'N'},
                                             {'AttributeName': 'range_key', 'AttributeType': 'S'}]
    assert table['KeySchema'] == [{'AttributeName': 'hash_key', 'KeyType': 'HASH'},
                                  {'AttributeName': 'range_key', 'KeyType': 'RANGE'}]
    assert table['ProvisionedThroughput']['ReadCapacityUnits'] == 3
    assert table['ProvisionedThroughput']['WriteCapacityUnits'] == 2
    #
    dynamodb.delete_table(TableName=table_name)
