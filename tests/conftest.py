import pytest
from settings import CONNECT_TO_AWS
from mock_wrapper import log_target, dynamodb, patch_boto, get_moto_services, verify_everything_is_deleted


@pytest.fixture()
def dynamodb_server_mode():
    # Same behaviour as the #mock_server_mode decorator
    # Used as a fixture, to ensure it plays nice with other fixtures (such as parametrize)
    if CONNECT_TO_AWS:
        log_target("AWS")
        yield dynamodb
    else:
        log_target("MOCK SERVER MODE")
        patch_boto()
        moto_services = get_moto_services(['dynamodb', 'lambda', 'iam'])

        yield moto_services['dynamodb']
        verify_everything_is_deleted(dynamodb=moto_services['dynamodb'],
                                     lmbda=moto_services['lambda'],
                                     iam=moto_services['iam'])
