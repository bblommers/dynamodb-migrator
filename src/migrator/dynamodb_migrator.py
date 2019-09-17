import boto3
from functools import wraps
from time import sleep


dynamodb = boto3.client('dynamodb')
function_list = []


def version(version_number):
    print('version('+str(version_number)+')')
    def inner_function(function):
        print('version('+str(version_number)+'):inner_function')
        @wraps(function)
        def wrapper(*args, **kwargs):
            print("Calling function that has version: " + str(version_number))
            function(*args, **kwargs)
        return wrapper
    return inner_function


def create(name):
    print("Name(" + name + ")")
    def inner_function(function):
        print("create("+name+"):inner_function")
        function_list.append({'name': name, 'func': function})
    return inner_function


def migrate():
    print(function_list)
    print('=================')
    for table in function_list:
        created_table = dynamodb.create_table(
            AttributeDefinitions=[{
                'AttributeName': 'somekey',
                'AttributeType': 'S'
            }],
            TableName=table['name'],
            KeySchema=[{
                'AttributeName': 'somekey',
                'KeyType': 'HASH'
            }],
            BillingMode='PAY_PER_REQUEST'
        )
        status = 'CREATING'
        while status != 'ACTIVE':
            created_table = dynamodb.describe_table(TableName=table['name'])['Table']
            status = created_table['TableStatus']
            sleep(1)
        table['func'](created_table)
    print('=================')
