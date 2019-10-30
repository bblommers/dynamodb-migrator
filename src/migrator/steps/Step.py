import boto3


class Step:
    _dynamodb = boto3.client('dynamodb')
    _metadata_table_name = 'dynamodb_migrator_metadata'
    _lambda = boto3.client('lambda')
    _iam = boto3.client('iam')
    _account_id = None
    _sts = boto3.client('sts')

    def execute(self):
        pass

    def get_region(self):
        my_session = boto3.session.Session()
        return my_session.region_name
