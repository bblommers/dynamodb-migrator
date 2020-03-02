import boto3
from migrator.utilities.Utilities import metadata_table_name

_accepted_table_properties = ['AttributeDefinitions',
                              'TableName',
                              'KeySchema',
                              'LocalSecondaryIndexes', 'GlobalSecondaryIndexes',
                              'BillingMode', 'ProvisionedThroughput',
                              'StreamSpecification',
                              'SSESpecification',
                              'Tags']
_accepted_index_properties = ["IndexName", "KeySchema", "Projection"]


class DynamoDButilities:

    def __init__(self, identifier, version):
        self._dynamodb = boto3.client('dynamodb')
        self._identifier = identifier
        self._version = str(version)

    @staticmethod
    def get_stream_props(table_name):
        return {'TableName': table_name,
                'StreamSpecification': {'StreamEnabled': True,
                                        'StreamViewType': 'NEW_AND_OLD_IMAGES'}}

    @staticmethod
    def get_table_creation_details(existing_table: dict, new_table_name: str,
                                   local_indexes: [dict], attr_definitions: [dict]):
        new_table = {key: existing_table[key] for key in existing_table if key in _accepted_table_properties}
        if 'LocalSecondaryIndexes' not in new_table:
            new_table['LocalSecondaryIndexes'] = []
        new_table['LocalSecondaryIndexes'] = [{k: props[k] for k in props if k in _accepted_index_properties}
                                              for props in new_table['LocalSecondaryIndexes']]
        new_table['LocalSecondaryIndexes'].extend(local_indexes)
        new_table['AttributeDefinitions'].extend(attr_definitions)
        if 'BillingModeSummary' in existing_table:
            new_table['BillingMode'] = existing_table['BillingModeSummary']['BillingMode']
            del new_table['ProvisionedThroughput']
        if 'ProvisionedThroughput' in new_table:
            del new_table['ProvisionedThroughput']['NumberOfDecreasesToday']
        new_table['TableName'] = new_table_name
        return new_table

    def _set_operation(self, operation, attr, name):
        self._dynamodb.update_item(
            TableName=metadata_table_name,
            Key={'identifier': {'S': self._identifier}},
            UpdateExpression=operation + " #v.#attr :val",
            ExpressionAttributeNames={'#v': self._version, '#attr': attr},
            ExpressionAttributeValues={":val": {"SS": [name]}})

    def get_attr(self, attr, version=None):
        _version = str(version) if version else self._version
        item = self._dynamodb.get_item(TableName=metadata_table_name,
                                       Key={'identifier': {'S': self._identifier}})['Item']
        return item[_version]['M'][attr]['SS'] if attr in item[_version]['M'] else []

    def add_table(self, name):
        self._set_operation("ADD", "tables", name)

    def add_policy(self, arn):
        self._set_operation("ADD", "policies", arn)

    def add_role(self, arn):
        self._set_operation("ADD", "roles", arn)

    def add_mapping(self, uuid):
        self._set_operation("ADD", "mappings", uuid)

    def add_function(self, arn):
        self._set_operation("ADD", "functions", arn)

    def add_attr_name(self, attr_name):
        self._set_operation("ADD", "attribute_name", attr_name)

    def remove_table(self, name):
        self._set_operation("DELETE", "tables", name)

    def remove_policy(self, arn):
        self._set_operation("DELETE", "policies", arn)

    def remove_role(self, arn):
        self._set_operation("DELETE", "roles", arn)

    def remove_mapping(self, uuid):
        self._set_operation("DELETE", "mappings", uuid)

    def remove_function(self, arn):
        self._set_operation("DELETE", "functions", arn)

    def get_created_tables(self, version=None):
        return self.get_attr("tables", version=version)

    def get_created_attr(self):
        return self.get_attr("attribute_name")

    def get_created_policies(self):
        return self.get_attr("policies")

    def get_created_roles(self):
        return self.get_attr("roles")

    def get_created_functions(self):
        return self.get_attr("functions")

    def get_created_mappings(self):
        return self.get_attr("mappings")
