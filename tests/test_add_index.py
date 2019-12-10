import logging
from time import sleep
from tenacity import before_sleep_log, retry, wait_exponential
from migrator.utilities.Utilities import logger, metadata_table_name
from mock_wrapper import aws_integration_test, mock_aws
from utilities import delete_tables
from uuid import uuid4

customer_nrs = [str(uuid4()) for _ in range(0, 10)]


@mock_aws
def test_add_index_script__assert_metadata_table_is_updated(dynamodb, lmbda, iam):
    import tests.migration_scripts.add_index.simple_index # noqa
    #
    # Ensure that all details are recorded in our metadata table
    metadata_table = dynamodb.scan(TableName=metadata_table_name)['Items'][0]
    assert metadata_table['identifier'] == {'S': 'simple_index'}
    assert metadata_table['2']['M']['tables']['SS'][0] == 'customers_V2'
    assert 'policies' in metadata_table['2']['M']
    assert 'roles' in metadata_table['2']['M']
    assert 'mappings' in metadata_table['2']['M']
    assert 'functions' in metadata_table['2']['M']
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
    #
    # Sleep for some time - make sure that the stream is up and running
    sleep(120)
    insert_random_data(dynamodb)
    #
    # Assert the new table has the items created in the first table
    try:
        verify_random_data(dynamodb)
    finally:
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


@aws_integration_test
def test_add_index_script__assert_existing_data_is_replicated(dynamodb, lmbda, iam):
    import migration_scripts.add_index.table_copy_items_v1  # noqa
    insert_random_data(dynamodb)
    # Update table
    import migration_scripts.add_index.table_copy_items_v2 # noqa
    #
    # Assert the new table has the items created in the first table
    try:
        verify_random_data(dynamodb)
    finally:
        #
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


def insert_random_data(dynamodb):
    for cust_nr in customer_nrs:
        dynamodb.put_item(TableName='customers',
                          Item={
                              'customer_nr': {'S': cust_nr},
                              'last_name': {'S': 'Smith'},
                              'postcode': {'S': 'PC12'}
                          })
    sleep(10)


@retry(wait=wait_exponential(multiplier=1, min=2, max=120), before_sleep=before_sleep_log(logger, logging.DEBUG))
def verify_random_data(dynamodb):
    items = dynamodb.scan(TableName='customers_V2')['Items']
    assert len(items) == len(customer_nrs)
    assert [item['customer_nr']['S'] for item in items].sort() == customer_nrs.sort()
    indexed_items = dynamodb.scan(TableName='customers_V2', IndexName='postcode_index')['Items']
    assert len(indexed_items) == len(customer_nrs)


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
    created_items = dynamodb.scan(TableName=metadata_table_name)['Items'][0]['2']['M']
    lmbda.delete_event_source_mapping(UUID=created_items['mappings']['SS'][0])
    lmbda.delete_function(FunctionName=created_items['functions']['SS'][0])
    role_arn = created_items['roles']['SS'][0]
    role_name = role_arn[role_arn.rindex('/') + 1:]
    iam.detach_role_policy(RoleName=role_name, PolicyArn=created_items['policies']['SS'][0])
    iam.delete_policy(PolicyArn=created_items['policies']['SS'][0])
    iam.delete_role(RoleName=role_name)
    delete_tables(dynamodb, [metadata_table_name, 'customers', 'customers_V2'])
