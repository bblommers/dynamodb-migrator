import boto3
import logging


class Step:
    _dynamodb = boto3.client('dynamodb')
    _metadata_table_name = 'dynamodb_migrator_metadata'
    _ch = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
    _logger = logging.getLogger('dynamodb_migrator_library')

    def __init__(self):
        self._ch.setFormatter(self._formatter)
        self._logger.addHandler(self._ch)
        self._logger.setLevel(logging.DEBUG)

    def execute(self):
        pass
