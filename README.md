# dynamodb-migrator 
[![Build Status](https://travis-ci.org/bblommers/dynamodb-migrator.svg?branch=master)](https://travis-ci.org/bblommers/dynamodb-migrator)
[![Coverage Status](https://coveralls.io/repos/github/bblommers/dynamodb-migrator/badge.svg?branch=master)](https://coveralls.io/github/bblommers/dynamodb-migrator?branch=master)
[![PyPI version](https://badge.fury.io/py/dynamodb-migrator.svg)](https://badge.fury.io/py/dynamodb-migrator)

A library that helps you create and migrate DynamoDB databases.

As performant DynamoDB is, that does come with the trade-off of being inflexible. Changing column names or adding secondary indexes is impossible.  
The recommended approach is to create a new table with the desired properties, and migrate the existing data.  
This library will help  you do just that.
  
 ## Usage
 - Write a migration script
 - Execute the migration script as a step in the build pipeline
 - Add to the migration-script as required
 
## Example Script
```python
from migrator.dynamodb_migrator import Migrator
migrator = Migrator()
@migrator.version(1)
@migrator.create(AttributeDefinitions=[{'AttributeName': 'hash_key', 'AttributeType': 'N'}],
                 TableName='my_new_table',
                 KeySchema=[{'AttributeName': 'hash_key', 'KeyType': 'HASH'}],
                 BillingMode='PAY_PER_REQUEST')
def v1(created_table):
    print("Table created using the kwargs provided")
    print("Note that the keyword-args are passed onto boto as is")
    print(created_table)


@migrator.version(2)
@migrator.add_index(AttributeDefinitions=[{'AttributeName': 'postcode', 'AttributeType': 'S'}],
                    LocalSecondaryIndexes=[{'IndexName': 'string',
                                            'KeySchema': [{'AttributeName': 'customer_nr', 'KeyType': 'HASH'},
                                                          {'AttributeName': 'postcode', 'KeyType': 'RANGE'}],
                                            'Projection': {'ProjectionType': 'ALL'}}])
def v2(created_table):
    print("Created a new table with the new index")
    print("Created a DynamoDB stream that sends all updates to the old table to a custom Lambda-function")
    print("The custom Lambda-function sends all updates to the new table")
    print(created_table)


@NotYetImplemented
@migrator.version(3)
@migrator.delete_table("first_table")
def v3(migrate):
    print("About to delete table")
    print("Ensure that all upstream applications point to the new table, before adding this part to the pipeline!")
    migrate()
    print("Table deleted")


@NotYetImplemented
@migrator.version(4)
@migrator.convert(lambda item -> {'id': translate(item.id)})
def v4(migrate):
    print("About to:")
    print(" - Create new table (first_table_v4)")
    print(" - Create a AWS Lambda script that will execute the above lambda, and write the result int he new table")
    print(" - Create DynamoDB Stream on 'first_table' that triggers the new Lambda")
    print(" - Execute a script that automatically updates all existing data")
    print("   (This will trigger all data in 'first_table' to be converted and copied into the new table")
    migrate()
    print("Table with new data is ready to use")

```

## Examples
See the [examples](examples)-folder.