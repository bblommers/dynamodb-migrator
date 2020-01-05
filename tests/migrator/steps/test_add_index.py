import importlib
import logging
import os
from datetime import datetime
from time import sleep
from random import randint
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from migrator.utilities.AwsUtilities import AwsUtilities
from migrator.utilities.LambdaUtilities import lambda_code, update_boto_client_endpoints, zip
from migrator.utilities.IAMutilities import lambda_stream_policy
from migrator.utilities.Utilities import logger, metadata_table_name
from mock_wrapper import aws_integration_test, mock_aws, mock_server_mode
from settings import CONNECT_TO_AWS
from delete_utilities import delete_tables
from uuid import uuid4

customer_nrs = [str(uuid4()) for _ in range(0, 3)]


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


# Moto does not allow mocked Lambda to access mocked DynamoDB - can only verify this in server mode
# https://github.com/spulec/moto/issues/1317
@mock_server_mode
def test_add_index_script__assert_data_is_send_through_for_multiple_versions(dynamodb, lmbda, iam):
    if not CONNECT_TO_AWS:
        # reload AWS utilities - make sure that boto3 is patched
        import migrator.utilities.AwsUtilities
        importlib.reload(migrator.utilities.AwsUtilities)
    import migration_scripts.add_index.table_stream_items_v1  # noqa
    from migration_scripts.add_index.table_stream_items_v1 import table_name
    try:
        # Create index, and verify data is transferred
        import migration_scripts.add_index.table_stream_items_v2  # noqa
        if not CONNECT_TO_AWS:
            # update Lambda when mocking, to point to local MOTO server
            update_dynamodb_host_in_lambda(dynamodb, lmbda, version='2')
        insert_and_verify_random_data(dynamodb, table_name, f'{table_name}_V2')
        # Create another index, and verify data is transferred again
        import migration_scripts.add_index.table_stream_items_v3  # noqa
        if not CONNECT_TO_AWS:
            update_dynamodb_host_in_lambda(dynamodb, lmbda, version='3')
        insert_and_verify_random_data(dynamodb, f'{table_name}_V2', f'{table_name}_V3')
    finally:
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


@aws_integration_test
def test_add_index_script__assert_existing_data_is_replicated(dynamodb, lmbda, iam):
    import migration_scripts.add_index.table_copy_items_v1  # noqa
    from migration_scripts.add_index.table_copy_items_v1 import table_name
    insert_random_data(dynamodb, table_name)
    # Update table
    import migration_scripts.add_index.table_copy_items_v2 # noqa
    #
    # Assert the new table has the items created in the first table
    try:
        reverify_random_data(dynamodb, f'{table_name}_V2')
    finally:
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)


def insert_random_data(dynamodb, table_name):
    for cust_nr in customer_nrs:
        dynamodb.put_item(TableName=table_name,
                          Item={
                              'customer_nr': {'S': cust_nr},
                              'last_name': {'S': 'Smith'},
                              'postcode': {'S': 'PC12'},
                              'loyalty_points': {'N': str(randint(0, 100))}
                          })
    sleep(10)


def update_random_data(dynamodb, table_name):
    items = dynamodb.scan(TableName=table_name)['Items']
    items = dynamodb.scan(TableName=metadata_table_name)['Items']
    unique_attr = items[0]['2']['M']['attribute_name']['SS'][0]
    for cust_nr in customer_nrs:
        key = {'customer_nr': {'S': cust_nr}, 'last_name': {'S': 'Smith'}}
        dynamodb.update_item(TableName=table_name,
                             Key=key,
                             UpdateExpression="set #attr = :val",
                             ExpressionAttributeNames={'#attr': unique_attr},
                             ExpressionAttributeValues={':val': {'S': str(datetime.today())}})
    sleep(10)


def verify_random_data(dynamodb, table_name):
    items = dynamodb.scan(TableName=table_name)['Items']
    assert len(items) == len(customer_nrs)
    assert [item['customer_nr']['S'] for item in items].sort() == customer_nrs.sort()
    indexed_items = dynamodb.scan(TableName=f'{table_name}', IndexName='postcode_index')['Items']
    assert len(indexed_items) == len(customer_nrs)


@retry(wait=wait_exponential(multiplier=1, min=2), stop=stop_after_attempt(5), before_sleep=before_sleep_log(logger, logging.DEBUG))
def reverify_random_data(dynamodb, table_name):
    verify_random_data(dynamodb, table_name)


@retry(wait=wait_exponential(multiplier=1, min=2, max=120), stop=stop_after_attempt(5), before_sleep=before_sleep_log(logger, logging.DEBUG))
def insert_and_verify_random_data(dynamodb, first_table, second_table):
    insert_random_data(dynamodb, first_table)
    verify_random_data(dynamodb, second_table)


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


@mock_server_mode
def test_add_index_script__assert_existing_streams_still_exist(dynamodb, lmbda, iam):
    if not CONNECT_TO_AWS:
        # reload AWS utilities - make sure that boto3 is patched
        import migrator.utilities.AwsUtilities
        importlib.reload(migrator.utilities.AwsUtilities)
    import migration_scripts.add_index.table_with_existing_stream_v1  # noqa
    aws_utils = AwsUtilities(identifier="table_with_existing_stream", version='1')
    try:
        # Create Lambda that listens to the stream
        table = dynamodb.describe_table(TableName='customers')['Table']
        policy_document = lambda_stream_policy.substitute(aws_utils.get_region(),
                                                          oldtable='*',
                                                          newtable='*')
        created_policy = aws_utils.create_policy('test_add_index_script__assert_existing_streams_still_exist', policy_document)
        created_role = aws_utils.create_role(desc='test_add_index_script__assert_existing_streams_still_exist')
        aws_utils.attach_policy_to_role(created_policy, created_role)
        func = aws_utils.create_aws_lambda(created_role, table_name='N/A')
        # Create stream
        mapping = aws_utils.create_event_source_mapping(stream_arn=table['LatestStreamArn'],
                                                        function_arn=func['FunctionArn'])
        # Create new table, and add a stream to the existing table
        import migration_scripts.add_index.table_with_existing_stream_v2  # noqa
        if not CONNECT_TO_AWS:
            # update Lambda when mocking, to point to local MOTO server
            update_dynamodb_host_in_lambda(dynamodb, lmbda, version='2')
        # Verify we now have to event sources, for our custom lambda and for the AddIndex step
        mappings = lmbda.list_event_source_mappings(EventSourceArn=table['LatestStreamArn'])
        assert len(mappings['EventSourceMappings']) == 2
    finally:
        delete_created_services(dynamodb=dynamodb, iam=iam, lmbda=lmbda)
        role_arn = created_role['Role']['Arn']
        role_name = role_arn[role_arn.rindex('/') + 1:]
        iam.detach_role_policy(RoleName=role_name, PolicyArn=created_policy['Policy']['Arn'])
        iam.delete_policy(PolicyArn=created_policy['Policy']['Arn'])
        iam.delete_role(RoleName=role_name)
        lmbda.delete_event_source_mapping(UUID=mapping['UUID'])
        lmbda.delete_function(FunctionName=func['FunctionArn'])


def update_dynamodb_host_in_lambda(dynamodb, lmbda, version):
    created_items = dynamodb.scan(TableName=metadata_table_name)['Items'][0][version]['M']
    created_function_arn = created_items['functions']['SS'][0]
    created_function_name = created_function_arn[created_function_arn.rindex(':') + 1:]
    created_table = dynamodb.describe_table(TableName=created_items['tables']['SS'][0])
    existing_code = lambda_code.substitute(newtable=created_table['Table']['TableName'])
    new_code = update_boto_client_endpoints(existing_code, str(os.environ['dynamodb_mock_endpoint_url']))
    res = lmbda.update_function_code(FunctionName=created_function_name, ZipFile=zip(new_code))
    lmbda.update_event_source_mapping(UUID=created_items['mappings']['SS'][0], FunctionName=res['FunctionArn'])
    sleep(10)


def delete_created_services(dynamodb, iam, lmbda):
    metadata = dynamodb.scan(TableName=metadata_table_name)['Items'][0]
    created_items = metadata['2']['M']
    tables = metadata['1']['M']['tables']['SS']
    tables.extend(created_items['tables']['SS'])
    delete(created_items, iam, lmbda)
    if '3' in metadata:
        created_items = dynamodb.scan(TableName=metadata_table_name)['Items'][0]['3']['M']
        delete(created_items, iam, lmbda)
        tables.extend(created_items['tables']['SS'])
    tables.append(metadata_table_name)
    delete_tables(dynamodb, tables)


def delete(created_items, iam, lmbda):
    lmbda.delete_event_source_mapping(UUID=created_items['mappings']['SS'][0])
    lmbda.delete_function(FunctionName=created_items['functions']['SS'][0])
    role_arn = created_items['roles']['SS'][0]
    role_name = role_arn[role_arn.rindex('/') + 1:]
    iam.detach_role_policy(RoleName=role_name, PolicyArn=created_items['policies']['SS'][0])
    iam.delete_policy(PolicyArn=created_items['policies']['SS'][0])
    iam.delete_role(RoleName=role_name)
