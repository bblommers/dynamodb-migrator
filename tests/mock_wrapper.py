import asyncio
import os
import boto3
import botocore
import logging
from moto import mock_dynamodb2, mock_sts, mock_lambda, mock_iam
from moto_server import MotoService, patch_boto
from settings import CONNECT_TO_AWS


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


def log_target(target):
    logger.info('====================================')
    logger.info(f'======== CONNECTING TO {target} =========')
    logger.info('====================================')


def verify_everything_is_deleted(dynamodb, lmbda, iam):
    verify_tables_are_deleted(dynamodb)
    verify_policies_are_deleted(iam)
    verify_roles_are_deleted(iam)
    verify_event_source_mappings_are_deleted(lmbda)
    verify_functions_are_deleted(lmbda)


def verify_tables_are_deleted(dynamodb):
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


def verify_policies_are_deleted(iam):
    policies = iam.list_policies(Scope='Local')['Policies']
    if policies:
        logger.error('====================================')
        logger.error('Please delete IAM policies after each test')
        logger.error(f'These policies have not been deleted  yet: {[policy["Arn"] for policy in policies]}')
        logger.error('Exiting')
        logger.error('====================================')
        raise Exception('Please delete IAM policies after each test')


def verify_roles_are_deleted(iam):
    roles = iam.list_roles()['Roles']
    if roles:
        logger.error('====================================')
        logger.error('Please delete IAM roles after each test')
        logger.error(f'These roles have not been deleted  yet: {[role["Arn"] for role in roles]}')
        logger.error('Exiting')
        logger.error('====================================')
        raise Exception('Please delete IAM roles after each test')


def verify_event_source_mappings_are_deleted(lmbda):
    mappings = lmbda.list_event_source_mappings()['EventSourceMappings']
    if mappings:
        logger.error('====================================')
        logger.error('Please delete Lambda event source mappings after each test')
        logger.error(f'Mapping for this event source have not been deleted  yet: {[mapping["EventSourceArn"] for mapping in mappings]}')
        logger.error('Exiting')
        logger.error('====================================')
        raise Exception('Please delete Lambda event source mappings after each test')


def verify_functions_are_deleted(lmbda):
    functions = lmbda.list_functions()['Functions']
    if functions:
        logger.error('====================================')
        logger.error('Please delete Lambda functions after each test')
        logger.error(f'These functions have not been deleted  yet: {[func["FunctionArn"] for func in functions]}')
        logger.error('Exiting')
        logger.error('====================================')
        raise Exception('Please delete Lambda functions after each test')


def mock_aws(func):
    def wrapper():
        if CONNECT_TO_AWS:
            log_target("AWS")
            func(dynamodb, lmbda, iam)
        else:
            log_target("MOCK")
            with mock_dynamodb2(), mock_lambda(), mock_iam(), mock_sts():
                func(dynamodb, lmbda, iam)
                verify_everything_is_deleted(dynamodb, lmbda, iam)

    return wrapper


def get_moto_services(boto_service_names):
    services = {}

    async def start_svc(svc_name):
        services[svc_name] = await MotoService(svc_name).__aenter__()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*[start_svc(svc_name) for svc_name in boto_service_names]))

    mock_env_vars = {'{}_mock_endpoint_url'.format(name): svc.endpoint_url + '/' for name, svc in services.items()}
    for name, value in mock_env_vars.items():
        os.environ[name] = value

    session = botocore.session.get_session()
    return {name: session.create_client(name) for name, _ in services.items()}


def mock_server_mode(func):
    def wrapper():
        if CONNECT_TO_AWS:
            log_target("AWS")
            func(dynamodb, lmbda, iam)
        else:
            log_target("MOCK SERVER MODE")
            patch_boto()
            moto_services = get_moto_services(['dynamodb', 'lambda', 'iam'])

            func(dynamodb=moto_services['dynamodb'],
                 lmbda=moto_services['lambda'],
                 iam=moto_services['iam'])
            verify_everything_is_deleted(dynamodb=moto_services['dynamodb'],
                                         lmbda=moto_services['lambda'],
                                         iam=moto_services['iam'])

    return wrapper


def aws_integration_test(func):
    def wrapper():
        if CONNECT_TO_AWS:
            log_target("AWS")
            func(dynamodb, lmbda, iam)
    return wrapper


def invalid_status(table_status):
    return table_status.upper() in ['CREATING', 'UPDATING', 'ACTIVE']
