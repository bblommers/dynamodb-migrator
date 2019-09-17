import os
import boto3
from moto import mock_dynamodb2

client = boto3.client('dynamodb')


def mock_aws(func):
    def wrapper():
        with mock_dynamodb2():
            func(client)
            table_names = client.list_tables()['TableNames']
            tables = [client.describe_table(TableName=name)['Table'] for name in table_names]
            non_deleted_tables = [table['TableName'] for table in tables if invalid_status(table['TableStatus'])]
            if non_deleted_tables:
                print('====================================')
                print('Please delete tables after each test')
                print('Exiting')
                print('====================================')
                return
        mock = os.environ['CONNECT_TO_AWS']
        if mock and (mock.lower() == 'yes'):
            print('====================================')
            print('======== CONNECTING TO AWS =========')
            print('====================================')
            func(client)
    return wrapper


def invalid_status(table_status):
    return table_status.upper() in ['CREATING', 'UPDATING', 'ACTIVE']
