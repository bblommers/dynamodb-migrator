import io
from string import Template
import zipfile


class LambdaUtilities():

    _lambda_code = Template("""import boto3
import json

dynamodb = boto3.client('dynamodb')
table_name = "$newtable"


def copy(event, context):
    print(event)
    for record in event['Records']:
        if record['eventName'] == 'REMOVE':
            response = dynamodb.delete_item(TableName=table_name,
                                 Key=record['dynamodb']['Keys'])
        if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
            response = dynamodb.put_item(TableName=table_name,
                              Item=record['dynamodb']['NewImage'])
    return {
        'statusCode': 200
    }
""")

    def __init__(self, lmbda):
        self._lambda = lmbda

    def create_aws_lambda(self, created_role, created_table, previous_table_name):
        f = io.BytesIO()
        z = zipfile.ZipFile(f, 'w', zipfile.ZIP_DEFLATED)
        info = zipfile.ZipInfo('lambda_stream.py')
        info.external_attr = 0o777 << 16  # give full access to included file
        z.writestr(info, self._lambda_code.substitute(newtable=created_table['TableName']))
        z.close()
        zipped_lambda_code = f.getvalue()
        func = self._lambda.create_function(FunctionName='dynamodb_migrator_' + previous_table_name,
                                            Runtime='python3.7',
                                            Role=created_role['Role']['Arn'],
                                            Handler='lambda_stream.copy',
                                            Code={'ZipFile': zipped_lambda_code})
        return func
