import boto3
import logging
from migrator.utilities.DynamoDButilities import DynamoDButilities
from migrator.utilities.IAMutilities import lambda_stream_assume_role, lambda_stream_policy
from migrator.utilities.LambdaUtilities import get_zipfile
from migrator.utilities.Utilities import logger, metadata_table_name, metadata_table_properties
from tenacity import before_sleep_log, retry, wait_exponential, stop_after_attempt, RetryError
from time import sleep
from uuid import uuid4

_dynamodb = boto3.client('dynamodb')
_lambda = boto3.client('lambda')
_iam = boto3.client('iam')
_sts = boto3.client('sts')


class AwsHistory:

    exponential = wait_exponential(multiplier=1, min=1, max=5)
    max_retry_attempts = 5
    stop = stop_after_attempt(max_retry_attempts)
    sleep_action = before_sleep_log(logger, logging.DEBUG)

    def __init__(self, identifier, version):
        self._identifier = identifier
        self._version = version
        self._ddb_utils = DynamoDButilities(identifier, version)

    def created_table(self, name):
        self._ddb_utils.add_table(name)

    def created_policy(self, arn):
        self._ddb_utils.add_policy(arn)

    def created_role(self, arn):
        self._ddb_utils.add_role(arn)

    def created_mapping(self, uuid):
        self._ddb_utils.add_mapping(uuid)

    def created_function(self, arn):
        self._ddb_utils.add_function(arn)

    def rollback(self):
        logger.warning("Something went wrong earlier on - rollback was initiated")
        tables = self._ddb_utils.get_created_tables()
        roles = self._ddb_utils.get_created_roles()
        policies = self._ddb_utils.get_created_policies()
        mappings = self._ddb_utils.get_created_mappings()
        functions = self._ddb_utils.get_created_functions()
        logger.warning(f"Deleting: Tables: {tables}")
        for name in tables:
            _dynamodb.delete_table(TableName=name)
            # Wait for table to be deleted
            while True:
                try:
                    _dynamodb.describe_table(TableName=name)
                    sleep(1)
                except _dynamodb.exceptions.ResourceNotFoundException:
                    self._ddb_utils.remove_table(name)
                    break
        logger.warning(f"Deleting: Roles: {roles}")
        for arn in roles:
            name = arn[arn.rindex('/') + 1:]
            attached_policies = [policy['PolicyArn'] for policy in _iam.list_attached_role_policies(RoleName=name)['AttachedPolicies']]
            # Detach any policies first
            for policy_arn in attached_policies:
                if policy_arn in policies:
                    _iam.detach_role_policy(RoleName=name, PolicyArn=policy_arn)
        # Then delete those policies
        logger.warning(f"Deleting: Policies: {policies}")
        for arn in policies:
            _iam.delete_policy(PolicyArn=arn)
            self._ddb_utils.remove_policy(arn)
        # And the roles
        logger.warning(f"Deleting: Roles: {roles}")
        for arn in roles:
            name = arn[arn.rindex('/') + 1:]
            _iam.delete_role(RoleName=name)
            self._ddb_utils.remove_role(arn)
        logger.warning(f"Deleting: EventSourceMappings: {mappings}")
        for uuid in mappings:
            _lambda.delete_event_source_mapping(UUID=uuid)
            self._ddb_utils.remove_mapping(name)
        logger.warning(f"Deleting: Functions: {functions}")
        for name in functions:
            _lambda.delete_function(FunctionName=name)
            self._ddb_utils.remove_function(name)


class AwsUtilities:

    _account_id = None
    ResourceNotFoundException = _dynamodb.exceptions.ResourceNotFoundException

    @retry(wait=AwsHistory.exponential, before_sleep=AwsHistory.sleep_action, stop=AwsHistory.stop)
    def retry(self, func, args):
        return func(*args)

    def __init__(self, identifier, version):
        my_session = boto3.session.Session()
        self._region = my_session.region_name
        self._identifier = identifier
        self._version = str(version)
        self._history = AwsHistory(self._identifier, self._version)

        initial_map = {'M': {}}
        try:
            self.describe_table(table_name=metadata_table_name)
            logger.debug(f"Metadata table '{metadata_table_name}' already exists")
            item = _dynamodb.get_item(TableName=metadata_table_name, Key={'identifier': {'S': identifier}})['Item']
            if str(version) not in item:
                _dynamodb.update_item(TableName=metadata_table_name,
                                      Key={'identifier': {'S': self._identifier}},
                                      UpdateExpression="set #attr = :val",
                                      ExpressionAttributeNames={'#attr': str(self._version)},
                                      ExpressionAttributeValues={':val': initial_map})
        except _dynamodb.exceptions.ResourceNotFoundException:
            logger.debug(f"Metadata table '{metadata_table_name}' does not exist yet")
            self._create_table(metadata_table_properties, keep_history=False)
            # Add version information
            _dynamodb.put_item(TableName=metadata_table_name, Item={'identifier': {'S': self._identifier},
                                                                    self._version: initial_map})

    def get_region(self):
        return self._region

    def create_table_if_not_exists(self, properties):
        try:
            return self.describe_table(properties['TableName'])['Table']
        except _dynamodb.exceptions.ResourceNotFoundException:
            return self.create_table(properties)

    def create_table(self, properties):
        """
        Creates a table in DynamoDB. Will retry/backoff if it doesnt succeed at first.
        Rolls back any changes made previously if it keeps failing.
        :param properties: DynamoDB table properties as specified by the AWS SDK
        :return: The details of the (ready-to-use) table
        """
        try:
            return self.retry(self._create_table, (properties, ))
        except RetryError as e:
            self._history.rollback()
            e.reraise()

    def _create_table(self, properties, keep_history=True):
        _dynamodb.create_table(**properties)
        status = 'CREATING'
        while status != 'ACTIVE':
            created_table = self.describe_table(properties['TableName'])['Table']
            status = created_table['TableStatus']
            sleep(1)
        logger.info(f"Successfully created table {properties['TableName']}")
        if keep_history:
            self._history.created_table(properties['TableName'])
        return created_table

    def describe_table(self, table_name):
        return _dynamodb.describe_table(TableName=table_name)

    def create_iam_items(self, created_table, previous_table):
        policy_document = lambda_stream_policy.substitute(region=self._region,
                                                          oldtable=previous_table['LatestStreamArn'],
                                                          newtable=created_table['TableArn'])
        desc = ' created by dynamodb_migrator, migrating data from ' + previous_table['TableName'] + ' to ' + created_table['TableName']
        created_policy = self.create_policy(desc, policy_document)
        created_role = self.create_role(desc=desc)
        self.attach_policy_to_role(created_policy, created_role)
        return created_policy, created_role

    def create_policy(self, desc, policy_document, policy_name=None):
        policy_name = policy_name or 'dynamodb_migrator_' + str(uuid4())[:4]

        def _create_policy():
            created_policy = _iam.create_policy(Path='/dynamodb_migrator/',
                                                PolicyName=policy_name,
                                                PolicyDocument=policy_document,
                                                Description='Policy' + desc)
            self._history.created_policy(created_policy['Policy']['Arn'])
            logger.info(f"Successfully created policy {policy_name}")
            return created_policy

        try:
            return self.retry(_create_policy, ())
        except RetryError as e:
            self._history.rollback()
            e.reraise()

    def create_role(self, desc, role_name=None):
        role_name = role_name or 'dynamodb_migrator_' + str(uuid4())[:4]

        def _create_role():
            created_role = _iam.create_role(Path='/dynamodb_migrator/',
                                            RoleName=role_name,
                                            AssumeRolePolicyDocument=lambda_stream_assume_role,
                                            Description='Role' + desc)
            self._history.created_role(created_role['Role']['Arn'])
            logger.info(f"Successfully created role {role_name}")
            return created_role
        try:
            return self.retry(_create_role, ())
        except RetryError as e:
            self._history.rollback()
            e.reraise()

    def attach_policy_to_role(self, created_policy, created_role):
        """
        Attaches a policy to a role. Will retry/backoff if it doesnt succeed at first.
        Rolls back any changes made previously if it keeps failing.
        :param created_policy:
        :param created_role:
        """
        def _attach_policy_to_role():
            _iam.attach_role_policy(PolicyArn=created_policy['Policy']['Arn'], RoleName=created_role['Role']['RoleName'])
        try:
            self.retry(_attach_policy_to_role, ())
        except RetryError as e:
            self._history.rollback()
            e.reraise()

    def create_aws_lambda(self, created_role, table_name, lambda_name = None):
        def _create_aws_lambda():
            zipped_lambda_code = get_zipfile(table_name=table_name)
            name = lambda_name or 'dynamodb_migrator_' + str(uuid4())[0:4]
            func = _lambda.create_function(FunctionName=name, Runtime='python3.7',
                                           Role=created_role['Role']['Arn'],
                                           Handler='lambda_stream.copy',
                                           Code={'ZipFile': zipped_lambda_code})
            self._history.created_function(func['FunctionArn'])
            logger.info(f"Successfully created function {name}")
            return func
        try:
            return self.retry(_create_aws_lambda, ())
        except RetryError as e:
            self._history.rollback()
            e.reraise()

    def create_event_source_mapping(self, stream_arn, function_arn):
        def _create_event_source_mapping():
            mapping = _lambda.create_event_source_mapping(EventSourceArn=stream_arn, FunctionName=function_arn,
                                                          Enabled=True,
                                                          BatchSize=10, MaximumBatchingWindowInSeconds=5,
                                                          StartingPosition='TRIM_HORIZON')
            while mapping['State'] != 'Enabled':
                mapping = _lambda.get_event_source_mapping(UUID=mapping['UUID'])
                sleep(1)
            self._history.created_mapping(mapping['UUID'])
            logger.info(f"Successfully created event_source_mapping {mapping['UUID']}")
            return mapping
        try:
            return self.retry(_create_event_source_mapping, ())
        except RetryError as e:
            self._history.rollback()
            e.reraise()

    def get_metadata_table(self):
        return _dynamodb.get_item(TableName=metadata_table_name,
                                  Key={'identifier': {'S': self._identifier}})['Item']
