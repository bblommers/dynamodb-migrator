from mock_wrapper import mock_aws
from delete_utilities import delete_tables
from migrator.utilities.AwsUtilities import AwsUtilities
from migrator.utilities.DynamoDButilities import DynamoDButilities
from migrator.utilities.Utilities import metadata_table_name
from uuid import uuid4


version = '1'


@mock_aws
def test_table_can_be_added(dynamodb, *_):
    identifier, utils = get_util(dynamodb)
    #
    utils.add_table("test_table1")
    utils.add_table("test_table2")
    assert get_metadata(dynamodb, identifier, 'tables') == ['test_table1', 'test_table2']
    assert sorted(utils.get_created_tables()) == ['test_table1', 'test_table2']
    #
    utils.remove_table("test_table1")
    assert get_metadata(dynamodb, identifier, 'tables') == ['test_table2']
    assert utils.get_created_tables() == ['test_table2']
    #
    # retrieve
    #
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_policy_can_be_added(dynamodb, *_):
    identifier, utils = get_util(dynamodb)
    #
    utils.add_policy("policy_arn1")
    utils.add_policy("policy_arn2")
    assert get_metadata(dynamodb, identifier, 'policies') == ['policy_arn1', 'policy_arn2']
    assert sorted(utils.get_created_policies()) == ['policy_arn1', 'policy_arn2']
    #
    utils.remove_policy("policy_arn1")
    assert get_metadata(dynamodb, identifier, 'policies') == ['policy_arn2']
    #
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_role_can_be_added(dynamodb, *_):
    identifier, utils = get_util(dynamodb)
    #
    utils.add_role("role_arn1")
    utils.add_role("role_arn2")
    assert get_metadata(dynamodb, identifier, 'roles') == ['role_arn1', 'role_arn2']
    assert sorted(utils.get_created_roles()) == ['role_arn1', 'role_arn2']
    #
    utils.remove_role("role_arn1")
    get_metadata(dynamodb, identifier, 'roles') == ['role_arn2']
    #
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_function_can_be_added(dynamodb, *_):
    identifier, utils = get_util(dynamodb)
    #
    utils.add_function("func_arn1")
    utils.add_function("func_arn2")
    assert get_metadata(dynamodb, identifier, 'functions') == ['func_arn1', 'func_arn2']
    assert sorted(utils.get_created_functions()) == ['func_arn1', 'func_arn2']
    #
    utils.remove_function("func_arn1")
    assert get_metadata(dynamodb, identifier, 'functions') == ['func_arn2']
    #
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_mapping_can_be_added(dynamodb, *_):
    identifier, utils = get_util(dynamodb)
    #
    utils.add_mapping("uuid1")
    utils.add_mapping("uuid2")
    assert get_metadata(dynamodb, identifier, 'mappings') == ['uuid1', 'uuid2']
    assert sorted(utils.get_created_mappings()) == ['uuid1', 'uuid2']
    #
    utils.remove_mapping("uuid1")
    assert get_metadata(dynamodb, identifier, 'mappings') == ['uuid2']
    #
    delete_tables(dynamodb, [metadata_table_name])


@mock_aws
def test_nonexisting_data_can_be_retrieved(dynamodb, *_):
    identifier, utils = get_util(dynamodb)
    #
    assert utils.get_created_functions() == []
    assert utils.get_created_mappings() == []
    assert utils.get_created_policies() == []
    assert utils.get_created_tables() == []
    assert utils.get_created_roles() == []
    #
    delete_tables(dynamodb, [metadata_table_name])


def get_util(dynamodb):
    identifier = str(uuid4())
    AwsUtilities(identifier=identifier, version=version)
    utils = DynamoDButilities(identifier=identifier, version=version)
    return identifier, utils


def get_metadata(dynamodb, identifier, _type):
    return sorted(dynamodb.get_item(TableName=metadata_table_name,
                                    Key={'identifier': {'S': identifier}})['Item'][version]['M'][_type]['SS'])
