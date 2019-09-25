import boto3
import logging
import os
from functools import wraps
from time import sleep


class Migrator():
    _dynamodb = boto3.client('dynamodb')
    _metadata_table_name = 'dynamodb_migrator_metadata'
    _ch = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
    _logger = logging.getLogger('dynamodb_migrator_library')

    def __init__(self, identifier = None):
        self._ch.setFormatter(self._formatter)
        self._logger.addHandler(self._ch)
        self._logger.setLevel(logging.DEBUG)
        self._function_list = []
        self._current_identifier = identifier if identifier else os.path.basename(__file__)
        self._get_or_create_metadata_table()

    def _get_or_create_metadata_table(self):
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

    def version(self, version_number):
        def inner_function(function):
            @wraps(function)
            def wrapper(*args, **kwargs):
                self.function(*args, **kwargs)
            return wrapper
        return inner_function

    def create(self, **kwargs):
        def inner_function(function):
            self._function_list.append({'identifier': self._current_identifier,
                                        'table_properties': kwargs,
                                        'func': function})
        return inner_function

    def migrate(self):
        if not self._function_list:
            self._logger.warning("No migration-steps have been found")
        for table in self._function_list:
            self._logger.debug(f"Creating table '{table['table_properties']['TableName']}'")
            table_name = table['table_properties']['TableName']
            if self._table_exists():
                self._logger.debug(f"Table '{self._current_identifier}' has already been created")
            else:
                created_table = self._dynamodb.create_table(**table['table_properties'])
                self._dynamodb.put_item(
                    TableName=self._metadata_table_name,
                    Item={
                        'identifier': {'S': self._current_identifier},
                        'version': {'N': "1"}
                    })
                status = 'CREATING'
                while status != 'ACTIVE':
                    created_table = self._dynamodb.describe_table(TableName=table_name)['Table']
                    status = created_table['TableStatus']
                    sleep(1)
                self._logger.info(f"Created table '{table['table_properties']['TableName']}'")
                table['func'](created_table)

    def _table_exists(self):
        curr_item = self._dynamodb.get_item(TableName=self._metadata_table_name,
                                            Key={'identifier': {'S': self._current_identifier}})
        return True if 'Item' in curr_item else False
