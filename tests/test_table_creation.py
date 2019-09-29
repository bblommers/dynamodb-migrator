from importlib import reload
from time import sleep
from .mock_wrapper import mock_aws
from migrator.exceptions.MigratorScriptException import MigratorScriptException


@mock_aws
def test_create_table_script__assert_table_is_created(dynamodb):
    from .migration_scripts import simple_table
    reload(simple_table)  # Ensure that this script isnt already loaded
    from .migration_scripts.simple_table import migrator
    migrator.migrate()
    from .migration_scripts.simple_table import table_name
    #
    # Assert the table is created
    table_names = dynamodb.list_tables()['TableNames']
    assert len(table_names) >= 1
    assert table_name in table_names
    #
    dynamodb.delete_table(TableName=table_name)
    delete_metadata_table(dynamodb)


@mock_aws
def test_create_table_script__assert_table_has_all_properties(dynamodb):
    from .migration_scripts import table_with_all_properties
    reload(table_with_all_properties)  # Ensure that this script isnt already loaded
    from .migration_scripts.table_with_all_properties import migrator
    migrator.migrate()
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
    delete_metadata_table(dynamodb)


@mock_aws
def test_create_table_script__assert_metadata_table_is_created(dynamodb):
    from .migration_scripts import static_table
    reload(static_table)  # Ensure that this script isnt already loaded
    from .migration_scripts.static_table import migrator
    migrator.migrate()
    from .migration_scripts.static_table import table_name
    #
    # Assert the table is created
    table_names = dynamodb.list_tables()['TableNames']
    assert len(table_names) >= 1
    assert 'dynamodb_migrator_metadata' in table_names
    #
    # Assert the correct metadata has been added
    metadata = dynamodb.scan(TableName='dynamodb_migrator_metadata')['Items']
    assert metadata == [{'identifier': {'S': 'dynamodb_migrator.py'},
                         'version': {'N': '1'}}]
    #
    dynamodb.delete_table(TableName=table_name)
    delete_metadata_table(dynamodb)


@mock_aws
def test_create_table_script__assert_error_when_there_are_multiple_create_statements(dynamodb):
    try:
        from .migration_scripts import multiple_create_table
        reload(multiple_create_table)  # Ensure that this script isnt already loaded
        assert True, "Script execution should fail, as only a single create-annotation per script is allowed"
    except MigratorScriptException:
        delete_metadata_table(dynamodb)


def delete_metadata_table(dynamodb):
    try:
        dynamodb.delete_table(TableName='dynamodb_migrator_metadata')
        while True:
            dynamodb.describe_table(TableName='dynamodb_migrator_metadata')
            sleep(1)
    except dynamodb.exceptions.ResourceNotFoundException:
        # Table might not exist (anymore)
        pass
