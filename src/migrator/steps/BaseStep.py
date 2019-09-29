from migrator.steps.Step import Step
from time import sleep


class BaseStep(Step):

    def execute(self):
        try:
            self._dynamodb.describe_table(TableName=self._metadata_table_name)
            self._logger.debug(f"Metadata table '{self._metadata_table_name}' already exists")
        except self._dynamodb.exceptions.ResourceNotFoundException:
            self._logger.debug(f"Metadata table '{self._metadata_table_name}' does not exist yet")
            self._dynamodb.create_table(
                AttributeDefinitions=[{
                    'AttributeName': 'identifier',
                    'AttributeType': 'S'
                }],
                TableName=self._metadata_table_name,
                KeySchema=[{
                    'AttributeName': 'identifier',
                    'KeyType': 'HASH'
                }],
                BillingMode='PAY_PER_REQUEST')
            status = 'CREATING'
            while status != 'ACTIVE':
                created_table = self._dynamodb.describe_table(TableName=self._metadata_table_name)['Table']
                status = created_table['TableStatus']
                sleep(1)
            self._logger.info(f"Metadata table '{self._metadata_table_name}' has been created")
