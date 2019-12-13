import io
import re
import zipfile
from string import Template


lambda_code = Template("""import boto3
import json

dynamodb = boto3.client('dynamodb')
table_name = "$newtable"


def copy(event, context):
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
    return zip(lambda_content)


def zip(content):
    f = io.BytesIO()
    z = zipfile.ZipFile(f, 'w', zipfile.ZIP_DEFLATED)
    info = zipfile.ZipInfo('lambda_stream.py')
    info.external_attr = 0o777 << 16  # give full access to included file
    z.writestr(info, content)
    z.close()
    return f.getvalue()


def update_boto_client_endpoints(original_code, endpoint_url):
    if original_code:
        # Set endpoint_url where there is none
        m = re.sub(r"""boto3.client\((('|")[a-zA-Z0-9]+('|"))\)""", rf"boto3.client(\1, endpoint_url='{endpoint_url}')", original_code)
        # Set endpoint url where another one was specified (potentially overwriting the first regex)
        m = re.sub(r"""boto3.client\((('|")[a-zA-Z0-9]+('|")), (endpoint_url=('|").+('|"))\)""", rf"boto3.client(\1, endpoint_url='{endpoint_url}')", m)
        return m
    return original_code
