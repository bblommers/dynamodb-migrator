from migrator.steps.Step import Step
from time import sleep


class CreateTableStep(Step):

    def __init__(self, identifier, version, properties):
        self._identifier = identifier
        self._version = version
        self._properties = properties

    def execute(self):
        table_name = self._properties['TableName']
        self._logger.debug(f"Creating table '{table_name}'")
        if self._table_exists():
            self._logger.debug(f"Table with identifier '{self._identifier}' has already been created")
            created_table = self._dynamodb.describe_table(TableName=table_name)['Table']
        else:
            self._properties['StreamSpecification'] = {'StreamEnabled': True,
                                                       'StreamViewType': 'NEW_AND_OLD_IMAGES'}
            created_table = self._dynamodb.create_table(**self._properties)
            self._dynamodb.put_item(
                TableName=self._metadata_table_name,
                Item={
                    'identifier': {'S': self._identifier},
                    str(self._version): {'S': table_name}
                })
            status = 'CREATING'
            while status != 'ACTIVE':
                created_table = self._dynamodb.describe_table(TableName=table_name)['Table']
                status = created_table['TableStatus']
                sleep(1)
            self._logger.info(f"Created table '{table_name}'")
        return created_table

    def _table_exists(self):
        curr_item = self._dynamodb.get_item(TableName=self._metadata_table_name,
                                            Key={'identifier': {'S': self._identifier}})
        return True if 'Item' in curr_item else False
