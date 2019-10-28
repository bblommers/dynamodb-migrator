from time import sleep
from mock_wrapper import aws_integration_test, mock_aws
from uuid import uuid4


@mock_aws
def test_add_index_script__assert_metadata_table_is_updated(dynamodb, lmbda, iam):
    import tests.migration_scripts.add_index.simple_index # noqa
    #
    # Ensure that all details are recorded in our metadata table
    metadata_table = dynamodb.scan(TableName='dynamodb_migrator_metadata')['Items'][0]
    assert metadata_table['identifier'] == {'S': 'simple_index'}
    assert metadata_table['1']['S'] == 'customers'
    assert metadata_table['2']['M']['table']['S'] == 'customers_V2'
    assert 'policy' in metadata_table['2']['M']
    assert 'role' in metadata_table['2']['M']
    assert 'role_name' in metadata_table['2']['M']
    assert 'stream' in metadata_table['2']['M']
    assert 'mapping' in metadata_table['2']['M']
    assert 'lambda' in metadata_table['2']['M']
    #
    # Assert the new table is created
    new_table = dynamodb.describe_table(TableName='customers_V2')['Table']
    assert new_table['ProvisionedThroughput']['ReadCapacityUnits'] == 1
    assert new_table['ProvisionedThroughput']['WriteCapacityUnits'] == 2
    assert len(new_table['LocalSecondaryIndexes']) == 1
    assert new_table['LocalSecondaryIndexes'][0]['IndexName'] == 'new_local_index'
    assert new_table['LocalSecondaryIndexes'][0]['KeySchema'] == [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                                                  {'AttributeName': 'postcode', 'KeyType': 'RANGE'}]
    assert 'GlobalSecondaryIndexes' not in new_table or new_table['GlobalSecondaryIndexes'] == []
    #
    delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


# Moto does not allow mocked Lambda to access mocked DynamoDB - can only verify this against AWS itself
# https://github.com/spulec/moto/issues/1317
@aws_integration_test
def test_add_index_script__assert_data_is_send_through(dynamodb, lmbda, iam):
    import migration_scripts.add_index.table_stream_items  # noqa
    cust_nr = str(uuid4())
    dynamodb.put_item(
        TableName='customers',
        Item={
            'customer_nr': {'S': cust_nr},
            'last_name': {'S': 'Smith'},
            'postcode': {'S': 'PC12'}
        })
    sleep(15)
    #
    # Assert the new table has the items created in the first table
    try:
        items = dynamodb.scan(TableName='customers_V2')['Items']
        assert items == [{'last_name': {'S': 'Smith'},
                          'customer_nr': {'S': cust_nr},
                          'postcode': {'S': 'PC12'}}]
        indexed_items = dynamodb.scan(TableName='customers_V2', IndexName='postcode_index')['Items']
        assert indexed_items == [{'last_name': {'S': 'Smith'},
                                  'postcode': {'S': 'PC12'},
                                  'customer_nr': {'S': cust_nr}}]
    finally:
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


@aws_integration_test
def test_add_index_script__assert_existing_data_is_replicated(dynamodb, lmbda, iam):
    import migration_scripts.add_index.table_copy_items_v1  # noqa
    customer_nrs = [str(uuid4()) for _ in range(0, 10)]
    for cust_nr in customer_nrs:
        dynamodb.put_item(TableName='customers',
                          Item={
                              'customer_nr': {'S': cust_nr},
                              'last_name': {'S': 'Smith'},
                              'postcode': {'S': 'PC12'}
                          })
    # Update table
    import migration_scripts.add_index.table_copy_items_v2 # noqa
    #
    # Assert the new table has the items created in the first table
    try:
        items = dynamodb.scan(TableName='customers_V2')['Items']
        assert len(items) == len(customer_nrs)
        assert [item['customer_nr']['S'] for item in items].sort() == customer_nrs.sort()
        indexed_items = dynamodb.scan(TableName='customers_V2', IndexName='postcode_index')['Items']
        assert len(indexed_items) == len(customer_nrs)
    finally:
        #
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


@mock_aws
def test_add_index_script__assert_existing_indexes_still_exist(dynamodb, lmbda, iam):
    import migration_scripts.add_index.table_with_existing_indexes  # noqa
    #
    # Assert the table is created
    table = dynamodb.describe_table(TableName='customers_V2')['Table']
    assert len(table['LocalSecondaryIndexes']) == 2
    assert [index['IndexName'] for index in table['LocalSecondaryIndexes']] == ['existing_index', 'new_index']
    #
    delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


@mock_aws
def test_add_index_script__assert_existing_streams_still_exist(dynamodb, lmbda, iam):
    ...


def delete_created_services(dynamodb, iam, lmbda):
    created_items = dynamodb.scan(TableName='dynamodb_migrator_metadata')['Items'][0]['2']['M']
    lmbda.delete_event_source_mapping(UUID=created_items['mapping']['S'])
    lmbda.delete_function(FunctionName=created_items['lambda']['S'])
    iam.detach_role_policy(RoleName=created_items['role_name']['S'], PolicyArn=created_items['policy']['S'])
    iam.delete_policy(PolicyArn=created_items['policy']['S'])
    iam.delete_role(RoleName=created_items['role_name']['S'])
    delete_tables(dynamodb, ['dynamodb_migrator_metadata', 'customers', 'customers_V2'])


def delete_tables(dynamodb, names):
    for name in names:
        delete_table(dynamodb, name)


def delete_table(dynamodb, name):
    try:
        dynamodb.delete_table(TableName=name)
        while True:
            dynamodb.describe_table(TableName=name)
            sleep(1)
    except dynamodb.exceptions.ResourceNotFoundException:
        # Table might not exist (anymore)
        pass


def delete_policies(iam, arns):
    for arn in arns:
        iam.delete_policy(PolicyArn=arn)


def delete_roles(iam, names):
    for name in names:
        iam.delete_role(RoleName=name)


def detach_role_policies(iam, role_policies):
    for role, policy in role_policies:
        iam.detach_role_policy(RoleName=role, PolicyArn=policy)


def delete_mappings(lmbda, mappings):
    for uuid in mappings:
        lmbda.delete_event_source_mapping(UUID=uuid)


def delete_functions(lmbda, functions):
    for name in functions:
        lmbda.delete_function(FunctionName=name)
