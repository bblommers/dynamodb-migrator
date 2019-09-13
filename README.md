# dynamodb-migrator

A library that helps you migrate DynamoDB databases.

As performant DynamoDB is, that does come with the trade-off of being very inflexible. Changing column names or adding secondary indexes is impossible.  
The recommended approach is to create a new table with the desired properties, and migrate the existing data.  
This library will help  you do just that.
  
 ## Usage
 - Write a migration script
 - Execute the migration script as a step in the build pipeline
 
## Example 
```python
@dynamodb_migrator.version(1)
@dynamodb_migrator.create("first_table")
def v1(migrate):
    console.log("About to create table...")
    migrate()
    console.log("Table created")


@dynamodb_migrator.version(2)
@dynamodb_migrator.add_index("secondary_index_we_forgot_about")
def v2(migrate):
    console.log("About to:")
    console.log(" - Create new table (first_table_v2) with the appropriate index")
    console.log(" - Create DynamoDB Stream on 'first_table' that copies changes into the new table")
    console.log(" - Execute a script that automatically updates all existing data")
    console.log("   (This will trigger all data in 'first_table' to be copied into the new table")
    migrate()
    console.log("Table with new index is ready to use")


@dynamodb_migrator.version(3)
@dynamodb_migrator.delete_table("first_table")
def v3(migrate):
    console.log("About to delete table")
    console.log("Ensure that all upstream applications point to the new table, before adding this part to the pipeline!")
    migrate()
    console.log("Table deleted")


@dynamodb_migrator.version(4)
@dynamodb_migrator.convert(lambda item -> {'id': translate(item.id)})
def v4(migrate):
    console.log("About to:")
    console.log(" - Create new table (first_table_v4)")
    console.log(" - Create a AWS Lambda script that will execute the above lambda, and write the result int he new table")
    console.log(" - Create DynamoDB Stream on 'first_table' that triggers the new Lambda")
    console.log(" - Execute a script that automatically updates all existing data")
    console.log("   (This will trigger all data in 'first_table' to be converted and copied into the new table")
    migrate()
    console.log("Table with new data is ready to use")

```