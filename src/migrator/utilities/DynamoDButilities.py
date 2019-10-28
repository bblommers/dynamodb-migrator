
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

    def get_table_creation_details(self, existing_table: dict, new_table_name: str,
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
