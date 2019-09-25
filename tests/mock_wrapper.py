import os
import boto3
import logging
from moto import mock_dynamodb2

client = boto3.client('dynamodb')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
logger = logging.getLogger('dynamodb_migrator_tests')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


def mock_aws(func):
    def wrapper():
        with mock_dynamodb2():
            func(client)
            table_names = client.list_tables()['TableNames']
            tables = [client.describe_table(TableName=name)['Table'] for name in table_names]
            non_deleted_tables = [table['TableName'] for table in tables if invalid_status(table['TableStatus'])]
            if non_deleted_tables:
                logger.error('====================================')
                logger.error('Please delete tables after each test')
                logger.error('Exiting')
                logger.error('====================================')
                raise Exception('Please delete tables after each test')
        mock = os.environ['CONNECT_TO_AWS']
        if mock and (mock.lower() == 'yes'):
            logger.debug('====================================')
            logger.debug('======== CONNECTING TO AWS =========')
            logger.debug('====================================')
            func(client)
    return wrapper


def invalid_status(table_status):
    return table_status.upper() in ['CREATING', 'UPDATING', 'ACTIVE']
