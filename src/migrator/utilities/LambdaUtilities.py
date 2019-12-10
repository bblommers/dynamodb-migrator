import io
import zipfile
from string import Template


lambda_code = Template("""import boto3
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


def get_zipfile(table_name):
    lambda_content = lambda_code.substitute(newtable=table_name)
    f = io.BytesIO()
    z = zipfile.ZipFile(f, 'w', zipfile.ZIP_DEFLATED)
    info = zipfile.ZipInfo('lambda_stream.py')
    info.external_attr = 0o777 << 16  # give full access to included file
    z.writestr(info, lambda_content)
    z.close()
    return f.getvalue()
