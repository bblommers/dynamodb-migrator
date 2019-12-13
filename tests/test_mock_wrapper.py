import boto3
from mock_wrapper import mock_server_mode
from moto import mock_dynamodb2
from settings import CONNECT_TO_AWS
from uuid import uuid4

table_name = str(uuid4())
table_properties = {'AttributeDefinitions': [{'AttributeName': 'identifier', 'AttributeType': 'S'}],
                    'TableName': table_name,
                    'KeySchema': [{'AttributeName': 'identifier', 'KeyType': 'HASH'}],
                    'BillingMode': 'PAY_PER_REQUEST',
                    'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}}


@mock_server_mode
def test_verify_server_mode_is_different_from_in_memory_mode(dynamodb, lmbda, iam):
    if CONNECT_TO_AWS:
        return  # No point in testing this against AWS itself
    # Assert we can create tables
    assert dynamodb.list_tables()['TableNames'] == []
    dynamodb.create_table(AttributeDefinitions=[{'AttributeName': 'identifier', 'AttributeType': 'S'}],
                          TableName=table_name,
                          KeySchema=[{'AttributeName': 'identifier', 'KeyType': 'HASH'}],
                          BillingMode='PAY_PER_REQUEST',
                          StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'})
    assert dynamodb.list_tables()['TableNames'] == [table_name]
    #
    # Assert that an in-memory mock cant reach server data
    with mock_dynamodb2():
        local_dynamodb = boto3.client('dynamodb')
        assert local_dynamodb.list_tables()['TableNames'] == []
