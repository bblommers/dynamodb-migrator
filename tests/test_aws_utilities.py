from botocore.exceptions import ClientError
from mock_wrapper import mock_aws
from migrator.utilities.AwsUtilities import AwsUtilities
from migrator.utilities.IAMutilities import lambda_stream_assume_role, lambda_stream_policy
from migrator.utilities.Utilities import metadata_table_name
from time import sleep
from uuid import uuid4
from utilities import delete_tables, delete_policies, delete_roles


table_name = str(uuid4())
table_properties = {'AttributeDefinitions': [{'AttributeName': 'identifier', 'AttributeType': 'S'}],
                    'TableName': table_name,
                    'KeySchema': [{'AttributeName': 'identifier', 'KeyType': 'HASH'}],
                    'BillingMode': 'PAY_PER_REQUEST',
                    'StreamSpecification': {'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}}


@mock_aws
def test_table_can_be_created(dynamodb, _, __):
    aws_util = AwsUtilities(identifier=str(uuid4()), version='1')
    aws_util.create_table(table_properties)
    #
    # Assert table exists with status 'ACTIVE'
    table_props = dynamodb.describe_table(TableName=table_name)
    assert table_props['Table']['TableName'] == table_name
    assert table_props['Table']['TableStatus'] == 'ACTIVE'
    #
    # Delete tables
    delete_tables(dynamodb, [metadata_table_name, table_name])


@mock_aws
def test_iam_policy_can_be_created_without_name(dynamodb, _, iam):
    aws_util = AwsUtilities(identifier=str(uuid4()), version='1')
    policy_document = lambda_stream_policy.substitute(region='', oldtable='*', newtable='*')
    policy = aws_util.create_policy(desc=str(uuid4()), policy_document=policy_document)
    #
    # Assert policy is created
    assert policy['Policy']['PolicyName'].startswith('dynamodb_migrator')
    all_policies = iam.list_policies(Scope='Local', PathPrefix='/dynamodb_migrator/')['Policies']
    assert policy['Policy']['PolicyName'] in [policy['PolicyName'] for policy in all_policies]
    #
    # Cleanup
    delete_tables(dynamodb, [metadata_table_name])
    delete_policies(iam, [policy['Policy']['Arn']])


@mock_aws
def test_iam_policy_can_be_created_with_name(dynamodb, _, iam):
    policy_name = str(uuid4())
    aws_util = AwsUtilities(identifier=str(uuid4()), version='1')
    policy_document = lambda_stream_policy.substitute(region='', oldtable='*', newtable='*')
    policy = aws_util.create_policy(desc=str(uuid4()), policy_document=policy_document, policy_name=policy_name)
    #
    # Assert policy is created
    assert policy['Policy']['PolicyName'] == policy_name
    all_policies = iam.list_policies(Scope='Local', PathPrefix='/dynamodb_migrator/')['Policies']
    assert policy_name in [policy['PolicyName'] for policy in all_policies]
    #
    # Cleanup
    delete_tables(dynamodb, [metadata_table_name])
    delete_policies(iam, [policy['Policy']['Arn']])


@mock_aws
def test_rollback_when_creating_two_tables(dynamodb, lmbda, iam):
    created_table = dynamodb.create_table(**table_properties)['TableDescription']
    status = 'CREATING'
    while status != 'ACTIVE':
        created_table = dynamodb.describe_table(TableName=table_name)['Table']
        status = created_table['TableStatus']
        sleep(1)
    # create something using AwsUtils
    identifier = str(uuid4())
    aws_util = AwsUtilities(identifier=identifier, version='1')
    aws_util.create_iam_items(created_table, created_table)
    # Sanity check that the policy now exists
    expected_iam_policy = get_recorded(dynamodb, identifier, "policies")[0]
    assert expected_iam_policy in [policy['Arn'] for policy in iam.list_policies(Scope='Local')['Policies']]
    # Try to recreate Table, this time using AwsUtils
    try:
        aws_util.create_table(table_properties)
        assert False, "Creating a table with the same name should not be allowed"
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            pass  # This is expected to fail, because a table with that name already exists
        else:
            raise e
    # assert original table still exists
    assert dynamodb.describe_table(TableName=table_name)['Table']['TableName'] == table_name
    # assert iam is deleted (which means the AwsUtils.rollback is working
    assert expected_iam_policy not in [policy['PolicyName'] for policy in iam.list_policies(Scope='Local')['Policies']]
    # Delete created table again
    delete_tables(dynamodb, [metadata_table_name, table_name])


@mock_aws
def test_rollback_when_creating_invalid_lambda(dynamodb, lmbda, iam):
    invalid_role = {'Role': {'Arn': 'nonsense'}}
    # create DynamoDB table using AwsUtils
    aws_util = AwsUtilities(identifier=str(uuid4()), version='1')
    aws_util.create_table(table_properties)
    assert table_name in dynamodb.list_tables()['TableNames']
    # Try to create Lambda, this time using AwsUtils
    try:
        aws_util.create_aws_lambda(invalid_role, 'table_name')
        assert False, "Creating AWS Lambda with an invalid role should fail"
    except ClientError as e:
        if e.response['Error']['Code'] == 'ValidationException':
            pass  # This is expected to fail, because a table with that name already exists
        else:
            raise e
    # assert DynamoDB Table is deleted (which means the AwsUtils.rollback is working
    assert table_name not in dynamodb.list_tables()['TableNames']
    # Delete created metadata table
    delete_tables(dynamodb, [metadata_table_name])


def create_function(created_role, lmbda, zipfile):
    lmbda.create_function(FunctionName='example_func', Runtime='python3.7',
                          Role=created_role['Role']['Arn'],
                          Handler='not.relevant', Code={'ZipFile': zipfile})


@mock_aws
def test_rollback_when_creating_two_roles(dynamodb, lmbda, iam):
    # create role
    name_postfix = 'example'
    rolename = 'dynamodb_migrator_' + name_postfix
    iam.create_role(RoleName=rolename, AssumeRolePolicyDocument=lambda_stream_assume_role)
    # create DynamoDB table using AwsUtils
    aws_util = AwsUtilities(identifier=str(uuid4()), version='1')
    aws_util.create_table(table_properties)
    try:
        # create role table using AwsUtils
        aws_util.create_role(role_name=rolename, desc=str(uuid4()))
        assert False, "Creating AWS IAM role with the same name should not succeed"
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            pass  # This is expected to fail, because a table with that name already exists
        else:
            raise e
    # assert role still exists
    assert iam.get_role(RoleName=rolename)['Role']['RoleName'] == rolename
    # assert DynamoDB Table is deleted (which means the AwsUtils.rollback is working
    assert table_name not in dynamodb.list_tables()['TableNames']
    # Delete created Lambda function manually
    delete_tables(dynamodb, [metadata_table_name])
    delete_roles(iam, [rolename])


@mock_aws
def test_rollback_iam_role_when_attachment_fails(dynamodb, lmbda, iam):
    # create role
    aws_util = AwsUtilities(identifier=str(uuid4()), version='1')

    role = aws_util.create_role(desc=str(uuid4()))
    try:
        # create role table using AwsUtils
        aws_util.attach_policy_to_role(None, role)
        assert False, "Attachment without IAM policy should not succeed"
    except TypeError as e:
        if str(e) == "'NoneType' object is not subscriptable":
            pass  # This is expected to fail, because we're passing in None
        else:
            raise e
    # assert created role was deleted (i.e., the failure succeeded in a rollback)
    assert [role for role in iam.list_roles(PathPrefix='/dynamodb_migrator/')['Roles']] == []
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_rollback_deletes_tables(dynamodb, *_):
    util = AwsUtilities(identifier=str(uuid4()), version='1')
    util.create_table(table_properties)
    util._history.rollback()
    #
    # Assert table is deleted
    assert table_name not in dynamodb.list_tables()['TableNames']
    #
    # Cleanup
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_aws_util_does_not_override_version_information(dynamodb, _, iam):
    identifier = str(uuid4())
    util = AwsUtilities(identifier=identifier, version=1)
    util.create_table(table_properties)
    #
    # assert metadata table has table information only
    assert get_recorded(dynamodb, identifier, "roles") == []
    assert get_recorded(dynamodb, identifier, "tables") == [table_name]
    #
    # Create a second thing with another aws_utils object
    util = AwsUtilities(identifier=identifier, version=1)
    role = util.create_role(desc=str(uuid4()))
    #
    # assert metadata table has table + role information
    assert get_recorded(dynamodb, identifier, "roles") == [role['Role']['Arn']]
    assert get_recorded(dynamodb, identifier, "tables") == [table_name]
    # Delete as appropriate
    delete_roles(iam, [role['Role']['RoleName']])
    delete_tables(dynamodb, [metadata_table_name, table_name])


@mock_aws
def test_aws_util_can_record_and_rollback_multiple_tables(dynamodb, lmbda, iam):
    second_table_name = str(uuid4())
    identifier = str(uuid4())
    util = AwsUtilities(identifier=identifier, version='1')
    util.create_table(table_properties)
    util.create_table({**table_properties, **{'TableName': second_table_name}})
    #
    # Assert tables are recorded
    assert all(name in dynamodb.list_tables()['TableNames'] for name in [table_name, second_table_name])
    assert get_recorded(dynamodb, identifier, "tables") == sorted([table_name, second_table_name])
    # Rollback
    # Assert tables and metadata are deleted
    util._history.rollback()
    assert all(name not in dynamodb.list_tables()['TableNames'] for name in [table_name, second_table_name])
    assert get_recorded(dynamodb, identifier, "tables") == []
    # Delete as appropriate
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_aws_util_can_record_and_rollback_multiple_policies(dynamodb, lmbda, iam):
    def get_policy_arns():
        return sorted([policy['Arn'] for policy in iam.list_policies(Scope='Local', PathPrefix='/dynamodb_migrator/')['Policies']])
    identifier = str(uuid4())
    util = AwsUtilities(identifier=identifier, version='1')
    policy_document = lambda_stream_policy.substitute(region='', oldtable='*', newtable='*')
    policy1 = util.create_policy(desc=str(uuid4()), policy_document=policy_document)['Policy']
    policy2 = util.create_policy(desc=str(uuid4()), policy_document=policy_document)['Policy']
    #
    # Assert policies are recorded
    assert get_policy_arns() == sorted([policy1['Arn'], policy2['Arn']])
    assert get_recorded(dynamodb, identifier, "policies") == sorted([policy1['Arn'], policy2["Arn"]])
    # Rollback
    # Assert policies and metadata are deleted
    util._history.rollback()
    assert get_policy_arns() == []
    assert get_recorded(dynamodb, identifier, "policies") == []
    # Delete as appropriate
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_aws_util_can_record_and_rollback_multiple_roles(dynamodb, lmbda, iam):
    def get_existing_roles():
        return sorted([role['Arn'] for role in iam.list_roles(PathPrefix='/dynamodb_migrator/')['Roles']])
    identifier = str(uuid4())
    util = AwsUtilities(identifier=identifier, version='1')
    role1 = util.create_role(desc=str(uuid4()))['Role']
    role2 = util.create_role(desc=str(uuid4()))['Role']
    #
    # Assert roles are recorded
    assert get_existing_roles() == sorted([role1['Arn'], role2['Arn']])
    assert get_recorded(dynamodb, identifier, "roles") == sorted([role1['Arn'], role2['Arn']])
    # Rollback
    # Assert policies and metadata are deleted
    util._history.rollback()
    assert get_existing_roles() == []
    assert get_recorded(dynamodb, identifier, "roles") == []
    # Delete as appropriate
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_aws_util_can_record_and_rollback_multiple_functions(dynamodb, lmbda, iam):
    def get_existing_functions():
        return sorted([fn['FunctionArn'] for fn in lmbda.list_functions()['Functions']])

    created_role = iam.create_role(RoleName=str(uuid4()), AssumeRolePolicyDocument=lambda_stream_assume_role)
    identifier = str(uuid4())
    util = AwsUtilities(identifier=identifier, version='1')
    fn1 = util.create_aws_lambda(created_role, table_name='ex')['FunctionArn']
    fn2 = util.create_aws_lambda(created_role, table_name='ex')['FunctionArn']
    #
    # Assert functions are recorded
    assert get_existing_functions() == sorted([fn1, fn2])
    assert get_recorded(dynamodb, identifier, "functions") == sorted([fn1, fn2])
    # Rollback
    # Assert policies and metadata are deleted
    util._history.rollback()
    assert get_existing_functions() == []
    assert get_recorded(dynamodb, identifier, "functions") == []
    # Delete as appropriate
    delete_tables(dynamodb, [metadata_table_name])
    delete_roles(iam, [created_role['Role']['RoleName']])


@mock_aws
def test_aws_util_removes_metadata_after_rollback(dynamodb, lmbda, iam):
    identifier = str(uuid4())
    util = AwsUtilities(identifier=identifier, version='1')
    util.create_table(table_properties)
    assert get_recorded(dynamodb, identifier, "tables") == [table_name]
    # Rollback - and make sure the metadata is also removed
    util._history.rollback()
    assert get_recorded(dynamodb, identifier, "tables") == []
    # Delete as appropriate
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_aws_util_create_table_if_not_exists(dynamodb, lmbda, iam):
    #
    # Initialize
    util = AwsUtilities(identifier=str(uuid4()), version='1')
    #
    # Create first table, and verify it exists
    response = util.create_table_if_not_exists(table_properties)
    assert response['TableName'] == table_name
    assert dynamodb.describe_table(TableName=table_name)['Table']['TableName'] == table_name
    #
    # Create it again   , and verify it exists
    response = util.create_table_if_not_exists(table_properties)
    assert response['TableName'] == table_name
    assert dynamodb.describe_table(TableName=table_name)['Table']['TableName'] == table_name
    #
    # Cleanup
    delete_tables(dynamodb, [metadata_table_name, table_name])


def get_metadata(dynamodb, identifier, version):
    return dynamodb.get_item(TableName=metadata_table_name, Key={'identifier': {'S': identifier}})['Item'][version]


def get_recorded(dynamodb, identifier, type):
    metadata = get_metadata(dynamodb, identifier, version='1')['M']
    if metadata and type in metadata:
        return sorted(metadata[type]['SS'])
    return []
