from migrator.steps.Step import Step
from migrator.utilities.DynamoDButilities import DynamoDButilities
from migrator.utilities.IAMutilities import IAMutilities
from migrator.utilities.LambdaUtilities import LambdaUtilities
from time import sleep


class AddIndexStep(Step):

    def __init__(self, identifier, version, properties):
        self._identifier = identifier
        self._version = version
        self._properties = properties
        self.ddb_utils = DynamoDButilities()
        self.iam_utils = IAMutilities(self._iam, region=self.get_region())
        self.lambda_utils = LambdaUtilities(self._lambda)
        super().__init__()

    def execute(self):
        self._logger.debug(f"Adding Index with properties '{self._properties}'")
        # TODO: Check whether table already exists
        previous_version = self._version - 1
        metadata = self._get_metadata()
        previous_table_name = metadata[str(previous_version)]['S']
        new_table_name = f"{previous_table_name}_V{self._version}"
        previous_table = self._dynamodb.describe_table(TableName=previous_table_name)['Table']
        new_table = self.ddb_utils.get_table_creation_details(previous_table, new_table_name,
                                                              local_indexes=self._properties['LocalSecondaryIndexes'],
                                                              attr_definitions=self._properties['AttributeDefinitions'])
        self._logger.debug(f"Creating new table with properties: {new_table}")
        # CREATE table based on old table
        created_table = self._dynamodb.create_table(**new_table)['TableDescription']
        status = 'CREATING'
        while status != 'ACTIVE':
            created_table = self._dynamodb.describe_table(TableName=new_table_name)['Table']
            status = created_table['TableStatus']
            sleep(1)
        # Create Role
        created_policy, created_role = self.iam_utils.create_iam_items(created_table, new_table_name,
                                                                       previous_table, previous_table_name)
        sleep(10)
        # Create Lambda
        func = self.lambda_utils.create_aws_lambda(created_role, created_table, previous_table_name)
        # Create stream
        mapping = self._lambda.create_event_source_mapping(EventSourceArn=previous_table['LatestStreamArn'],
                                                           FunctionName=func['FunctionArn'],
                                                           Enabled=True,
                                                           BatchSize=10,
                                                           MaximumBatchingWindowInSeconds=5,
                                                           StartingPosition='TRIM_HORIZON')
        while mapping['State'] != 'Enabled':
            mapping = self._lambda.get_event_source_mapping(UUID=mapping['UUID'])
            sleep(1)
        self._logger.info(f"Created stream: {mapping}")
        sleep(120)
        self._logger.info(f"Created table {new_table_name}")
        # Update metadata table
        self._dynamodb.update_item(
            TableName=self._metadata_table_name,
            Key={
                'identifier': {'S': self._identifier}
            },
            UpdateExpression="set #attr = :val",
            ExpressionAttributeNames={'#attr': str(self._version)},
            ExpressionAttributeValues={':val': {'M': {'table': {'S': new_table_name},
                                                      'policy': {'S': created_policy['Policy']['Arn']},
                                                      'role': {'S': created_role['Role']['Arn']},
                                                      'role_name': {'S': created_role['Role']['RoleName']},
                                                      'stream': {'S': previous_table['LatestStreamArn']},
                                                      'mapping': {'S': mapping['UUID']},
                                                      'lambda': {'S': func['FunctionArn']}}}}
        )
        return created_table

    def _get_metadata(self):
        return self._dynamodb.get_item(TableName=self._metadata_table_name,
                                       Key={'identifier': {'S': self._identifier}})['Item']
