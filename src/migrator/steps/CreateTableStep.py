from migrator.steps.Step import Step
from time import sleep


class CreateTableStep(Step):

    def __init__(self, identifier, properties, func):
        self._identifier = identifier
        self._properties = properties
        self._func = func

    def execute(self):
        self._logger.debug(f"Creating table '{self._properties['TableName']}'")
        table_name = self._properties['TableName']
        if self._table_exists():
            self._logger.debug(f"Table with identifier '{self._identifier}' has already been created")
        else:
            created_table = self._dynamodb.create_table(**self._properties)
            self._dynamodb.put_item(
                TableName=self._metadata_table_name,
                Item={
                    'identifier': {'S': self._identifier},
                    'version': {'N': "1"}
                })
            status = 'CREATING'
            while status != 'ACTIVE':
                created_table = self._dynamodb.describe_table(TableName=table_name)['Table']
                status = created_table['TableStatus']
                sleep(1)
            self._logger.info(f"Created table '{self._properties['TableName']}'")
            self._func(created_table)

    def _table_exists(self):
        curr_item = self._dynamodb.get_item(TableName=self._metadata_table_name,
                                            Key={'identifier': {'S': self._identifier}})
        return True if 'Item' in curr_item else False
