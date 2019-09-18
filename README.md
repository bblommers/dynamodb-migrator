# dynamodb-migrator

A library that helps you create and migrate DynamoDB databases.

As performant DynamoDB is, that does come with the trade-off of being inflexible. Changing column names or adding secondary indexes is impossible.  
The recommended approach is to create a new table with the desired properties, and migrate the existing data.  
This library will help  you do just that.
  
 ## Usage
 - Write a migration script
 - Execute the migration script as a step in the build pipeline
 - Add to the migration-script as required
 
## Example 
```python
@dynamodb_migrator.version(1)
@dynamodb_migrator.create(TableName='my_first_table')
def v1(created_table):
    print("Table created using the kwargs provided")
    print("Note that the keyword-args are passed onto boto as is")
    print(created_table)


@NotYetImplemented
@dynamodb_migrator.version(2)
@dynamodb_migrator.add_index("secondary_index_we_forgot_about")
def v2(migrate):
    print("About to:")
    print(" - Create new table (first_table_v2) with the appropriate index")
    print(" - Create DynamoDB Stream on 'first_table' that copies changes into the new table")
    print(" - Execute a script that automatically updates all existing data")
    print("   (This will trigger all data in 'first_table' to be copied into the new table")
    migrate()
    print("Table with new index is ready to use")


@NotYetImplemented
@dynamodb_migrator.version(3)
@dynamodb_migrator.delete_table("first_table")
def v3(migrate):
    print("About to delete table")
    print("Ensure that all upstream applications point to the new table, before adding this part to the pipeline!")
    migrate()
    print("Table deleted")


@NotYetImplemented
@dynamodb_migrator.version(4)
@dynamodb_migrator.convert(lambda item -> {'id': translate(item.id)})
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