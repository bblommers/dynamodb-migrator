import logging


_ch = logging.StreamHandler()
_formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
logger = logging.getLogger('dynamodb_migrator_library')
_ch.setFormatter(_formatter)
logger.addHandler(_ch)
logger.setLevel(logging.DEBUG)

metadata_table_name = 'dynamodb_migrator_metadata'
metadata_table_properties = {'AttributeDefinitions': [{'AttributeName': 'identifier', 'AttributeType': 'S'}],
                             'TableName': metadata_table_name,
                             'KeySchema': [{'AttributeName': 'identifier', 'KeyType': 'HASH'}],
                             'BillingMode': 'PAY_PER_REQUEST'}
