from time import sleep
from string import Template


class IAMutilities():

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

    def __init__(self, iam, region):
        self._iam = iam
        self.region = region

    def create_iam_items(self, created_table, new_table_name, previous_table, previous_table_name):
        policy_document = self._lambda_stream_policy.substitute(region=self.region,
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
        return created_policy, created_role
