import io
import zipfile
from migrator.steps.Step import Step
from string import Template
from time import sleep


class AddIndexStep(Step):

    _accepted_table_properties = ['AttributeDefinitions',
                                  'TableName',
                                  'KeySchema',
                                  'LocalSecondaryIndexes', 'GlobalSecondaryIndexes',
                                  'BillingMode', 'ProvisionedThroughput',
                                  'StreamSpecification',
                                  'SSESpecification',
                                  'Tags']
    _accepted_index_properties = ["IndexName", "KeySchema", "Projection"]

    _lambda_stream_policy = Template("""{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "dynamodb:PutItem",
            "dynamodb:DeleteItem",
            "dynamodb:UpdateItem"
        ],
        "Resource": "$newtable"
    }, {
        "Effect": "Allow",
        "Action": [
            "dynamodb:GetShardIterator",
            "dynamodb:DescribeStream",
            "dynamodb:ListStreams",
            "dynamodb:GetRecords"
        ],
        "Resource": "$oldtable"
    }, {
        "Effect": "Allow",
        "Action": "logs:*",
        "Resource": "*"
    }
]}""")

    _lambda_stream_assume_role = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""

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

    def __init__(self, identifier, version, properties):
        self._identifier = identifier
        self._version = version
        self._properties = properties
        super().__init__()

    def execute(self):
        self._logger.debug(f"Adding Index with properties '{self._properties}'")
        # TODO: Check whether table already exists
        previous_version = self._version - 1
        metadata = self._get_metadata()
        previous_table_name = metadata[str(previous_version)]['S']
        new_table_name = f"{previous_table_name}_V{self._version}"
        previous_table = self._dynamodb.describe_table(TableName=previous_table_name)['Table']
        new_table = {key: previous_table[key] for key in previous_table if key in self._accepted_table_properties}
        if 'LocalSecondaryIndexes' not in new_table:
            new_table['LocalSecondaryIndexes'] = []
        new_table['LocalSecondaryIndexes'] = [{k:props[k] for k in props if k in self._accepted_index_properties}
                                              for props in new_table['LocalSecondaryIndexes']]
        new_table['LocalSecondaryIndexes'].extend(self._properties['LocalSecondaryIndexes'])
        new_table['AttributeDefinitions'].extend(self._properties['AttributeDefinitions'])
        if 'BillingModeSummary' in previous_table:
            new_table['BillingMode'] = previous_table['BillingModeSummary']['BillingMode']
            del new_table['ProvisionedThroughput']
        if 'ProvisionedThroughput' in new_table:
            del new_table['ProvisionedThroughput']['NumberOfDecreasesToday']
        new_table['TableName'] = new_table_name
        self._logger.debug(f"Creating new table with properties: {new_table}")
        # CREATE table based on old table
        created_table = self._dynamodb.create_table(**new_table)['TableDescription']
        status = 'CREATING'
        while status != 'ACTIVE':
            created_table = self._dynamodb.describe_table(TableName=new_table_name)['Table']
            status = created_table['TableStatus']
            sleep(1)
        # Create Role
        policy_document = self._lambda_stream_policy.substitute(region=self.get_region(),
                                                                oldtable=previous_table['LatestStreamArn'],
                                                                newtable=created_table['TableArn'])
        desc = ' created by dynamodb_migrator, migrating data from ' + previous_table_name + ' to ' + new_table_name
        created_policy = self._iam.create_policy(PolicyName='dynamodb_migrator_' + previous_table_name,
                                                 PolicyDocument=policy_document,
                                                 Description='Policy' + desc)
        created_role = self._iam.create_role(RoleName='dynamodb_migrator_' + previous_table_name,
                                             AssumeRolePolicyDocument=self._lambda_stream_assume_role,
                                             Description='Role' + desc)
        sleep(15)
        self._iam.attach_role_policy(PolicyArn=created_policy['Policy']['Arn'],
                                     RoleName=created_role['Role']['RoleName'])
        sleep(10)
        # Create Lambda
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
        # Create stream
        mapping = self._lambda.create_event_source_mapping(EventSourceArn=previous_table['LatestStreamArn'],
                                                           FunctionName=func['FunctionArn'],
                                                           Enabled=True,
                                                           BatchSize=10,
                                                           MaximumBatchingWindowInSeconds=5,
                                                           StartingPosition='TRIM_HORIZON')
        while mapping['State'] != 'Enabled':
            mapping = self._lambda.get_event_source_mapping(UUID=mapping['UUID'])
            sleep(1)
        self._logger.info(f"Created stream: {mapping}")
        sleep(120)
        self._logger.info(f"Created table {new_table_name}")
        # Update metadata table
        self._dynamodb.update_item(
            TableName=self._metadata_table_name,
            Key={
                'identifier': {'S': self._identifier}
            },
            UpdateExpression="set #attr = :val",
            ExpressionAttributeNames={'#attr': str(self._version)},
            ExpressionAttributeValues={':val': {'M': {'table': {'S': new_table_name},
                                                      'policy': {'S': created_policy['Policy']['Arn']},
                                                      'role': {'S': created_role['Role']['Arn']},
                                                      'role_name': {'S': created_role['Role']['RoleName']},
                                                      'stream': {'S': previous_table['LatestStreamArn']},
                                                      'mapping': {'S': mapping['UUID']},
                                                      'lambda': {'S': func['FunctionArn']}}}}
        )
        return created_table

    def _get_metadata(self):
        return self._dynamodb.get_item(TableName=self._metadata_table_name,
                                       Key={'identifier': {'S': self._identifier}})['Item']
