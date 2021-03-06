from mock_wrapper import mock_aws
from migrator.exceptions.MigratorScriptException import MigratorScriptException
from migrator.utilities.Utilities import metadata_table_name
from delete_utilities import delete_tables


@mock_aws
def test_create_table_script__assert_table_is_created(dynamodb, lmbda, iam):
    import migration_scripts.create.simple_table # noqa
    from migration_scripts.create.simple_table import table_name
    #
    # Assert the table is created
    table_names = dynamodb.list_tables()['TableNames']
    assert len(table_names) >= 1
    assert table_name in table_names
    #
    delete_tables(dynamodb, [table_name, metadata_table_name])


@mock_aws
def test_create_table_script__assert_table_has_all_properties(dynamodb, lmbda, iam):
    import migration_scripts.create.table_with_all_properties # noqa
    from migration_scripts.create.table_with_all_properties import table_name
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
    delete_tables(dynamodb, [table_name, metadata_table_name])


@mock_aws
def test_create_table_script__assert_metadata_table_is_created(dynamodb, lmbda, iam):
    import migration_scripts.create.static_table # noqa
    from migration_scripts.create.static_table import table_name
    #
    # Assert the table is created
    table_names = dynamodb.list_tables()['TableNames']
    assert len(table_names) >= 1
    assert metadata_table_name in table_names
    #
    # Assert the correct metadata has been added
    metadata = dynamodb.scan(TableName=metadata_table_name)['Items']
    assert len(metadata) == 1
    assert 'identifier' in metadata[0]
    assert metadata[0]['identifier']['S'] == 'dynamodb_migrator.py'
    assert '1' in metadata[0]
    assert metadata[0]['1'] == {'M': {'tables': {'SS': [table_name]}}}
    #
    delete_tables(dynamodb, [table_name, metadata_table_name])


@mock_aws
def test_create_table_script__assert_error_when_there_are_multiple_create_statements(dynamodb, lmbda, iam):
    try:
        import migration_scripts.create.multiple_create_table # noqa
        assert False, "Script execution should fail, as only a single create-annotation per script is allowed"
    except MigratorScriptException:
        delete_tables(dynamodb, ['multiple_create_table_1', metadata_table_name])
