from migrator.utilities.AwsUtilities import AwsUtilities
from migrator.utilities.DynamoDButilities import DynamoDButilities
from migrator.utilities.Utilities import logger


class AddIndexStep:

    def __init__(self, identifier, version, properties):
        self._identifier = identifier
        self._version = version
        self._properties = properties
        self.aws_utils = AwsUtilities(identifier=self._identifier, version=self._version)
        self.ddb_utils = DynamoDButilities(identifier=self._identifier, version=self._version)

    def execute(self):
        logger.debug(f"Adding Index with properties '{self._properties}'")
        # TODO: Check whether table already exists
        previous_version = self._version - 1
        previous_table_name = self.ddb_utils.get_created_tables(version=previous_version)[0]
        new_table_name = f"{previous_table_name}_V{self._version}"
        previous_table = self.aws_utils.describe_table(previous_table_name)['Table']
        new_table = DynamoDButilities.get_table_creation_details(previous_table, new_table_name,
                                                                 local_indexes=self._properties['LocalSecondaryIndexes'],
                                                                 attr_definitions=self._properties['AttributeDefinitions'])
        logger.debug(f"Creating new table with properties: {new_table}")
        # CREATE table based on old table
        created_table = self.aws_utils.create_table(new_table)
        # Create Role
        created_policy, created_role = self.aws_utils.create_iam_items(created_table, previous_table)
        # Create Lambda
        func = self.aws_utils.create_aws_lambda(created_role, created_table['TableName'])
        # Create stream
        self.aws_utils.create_event_source_mapping(stream_arn=previous_table['LatestStreamArn'],
                                                   function_arn=func['FunctionArn'])
        return created_table
