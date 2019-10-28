import os
import boto3
import logging
from moto import mock_dynamodb2, mock_sts, mock_lambda, mock_iam

dynamodb = boto3.client('dynamodb')
lmbda = boto3.client('lambda')
iam = boto3.client('iam')
sts = boto3.client('sts')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
logger = logging.getLogger('dynamodb_migrator_tests')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


def mock_aws(func):
    def wrapper():
        mock = os.environ['CONNECT_TO_AWS']
        if not mock or (mock.lower() == 'no'):
            logger.info('==============================')
            logger.info('======== MOCKING AWS =========')
            logger.info('==============================')
            with mock_dynamodb2(), mock_lambda(), mock_iam(), mock_sts():
                func(dynamodb, lmbda, iam)
                verify_tables_are_deleted()
                verify_policies_are_deleted()
                verify_roles_are_deleted()
                verify_event_source_mappings_are_deleted()
                verify_functions_are_deleted()
        elif mock and (mock.lower() == 'yes'):
            logger.info('====================================')
            logger.info('======== CONNECTING TO AWS =========')
            logger.info('====================================')
            func(dynamodb, lmbda, iam)

    def verify_tables_are_deleted():
        table_names = dynamodb.list_tables()['TableNames']
        tables = [dynamodb.describe_table(TableName=name)['Table'] for name in table_names]
        non_deleted_tables = [table['TableName'] for table in tables if invalid_status(table['TableStatus'])]
        if non_deleted_tables:
            logger.error('====================================')
            logger.error('Please delete DynamoDB tables after each test')
            logger.error(f'These tables have not been deleted  yet: {non_deleted_tables}')
            logger.error('Exiting')
            logger.error('====================================')
            raise Exception('Please delete DynamoDB tables after each test')

    def verify_policies_are_deleted():
        policies = iam.list_policies(Scope='Local')['Policies']
        if policies:
            logger.error('====================================')
            logger.error('Please delete IAM policies after each test')
            logger.error(f'These policies have not been deleted  yet: {[policy["Arn"] for policy in policies]}')
            logger.error('Exiting')
            logger.error('====================================')
            raise Exception('Please delete IAM policies after each test')

    def verify_roles_are_deleted():
        roles = iam.list_roles()['Roles']
        if roles:
            logger.error('====================================')
            logger.error('Please delete IAM roles after each test')
            logger.error(f'These roles have not been deleted  yet: {[role["Arn"] for role in roles]}')
            logger.error('Exiting')
            logger.error('====================================')
            raise Exception('Please delete IAM roles after each test')

    def verify_event_source_mappings_are_deleted():
        mappings = lmbda.list_event_source_mappings()['EventSourceMappings']
        if mappings:
            logger.error('====================================')
            logger.error('Please delete Lambda event source mappings after each test')
            logger.error(f'Mapping for this event source have not been deleted  yet: {[mapping["EventSourceArn"] for mapping in mappings]}')
            logger.error('Exiting')
            logger.error('====================================')
            raise Exception('Please delete Lambda event source mappings after each test')

    def verify_functions_are_deleted():
        functions = lmbda.list_functions()['Functions']
        if functions:
            logger.error('====================================')
            logger.error('Please delete Lambda functions after each test')
            logger.error(f'These functions have not been deleted  yet: {[func["FunctionArn"] for func in functions]}')
            logger.error('Exiting')
            logger.error('====================================')
            raise Exception('Please delete Lambda functions after each test')

    return wrapper


def aws_integration_test(func):
    def wrapper():
        mock = os.environ['CONNECT_TO_AWS']
        if mock and (mock.lower() == 'yes'):
            logger.info('====================================')
            logger.info('======== CONNECTING TO AWS =========')
            logger.info('====================================')
            func(dynamodb, lmbda, iam)
    return wrapper


def invalid_status(table_status):
    return table_status.upper() in ['CREATING', 'UPDATING', 'ACTIVE']
