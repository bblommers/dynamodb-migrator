import boto3
from functools import wraps
from time import sleep


dynamodb = boto3.client('dynamodb')
_function_list = []


def version(version_number):
    def inner_function(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            function(*args, **kwargs)
        return wrapper
    return inner_function


def create(**kwargs):
    def inner_function(function):
        _function_list.append({'table_properties': kwargs, 'func': function})
    return inner_function


def migrate():
    for table in _function_list:
        table_name = table['table_properties']['TableName']
        created_table = dynamodb.create_table(**table['table_properties'])
        status = 'CREATING'
        while status != 'ACTIVE':
            created_table = dynamodb.describe_table(TableName=table_name)['Table']
            status = created_table['TableStatus']
            sleep(1)
        table['func'](created_table)
