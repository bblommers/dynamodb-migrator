# FAQ


### What?
A library that helps you create and migrate DynamoDB databases.

### Why?
Changing DynamoDB tables is impossible post-creation.  
The commonly accepted 'workaround' is to create a new table that does have the correct properties, and migrate the existing data.  
This tool will help you do that.

### How?
See the [examples](examples)-folder for up-to-date example scripts, and example code how to incorporate this in your CI/CD system.


### Can I contribute?
Definitely! Feel free to report bugs, recommend new features or open a PR.


### How do I....
##### Create multiple tables?
Each script is responsible for a single table. Create multiple tables by writing multiple scripts.

#### Clean up?
The _dynamodb_metadata_-table lists all AWS resources that have been created when running the script.  

#### Rollback?
If an AWS calls fails, for instance because the script does not have the required permissions, a rollback is executed automatically.  

For instance, this is the expected workflow:
 1. Create a DynamoDB table
 2. Create an IAM role
 3. Create an AWS Lambda function  

Due to incorrect permissions, this could be an actual workflow:
1. Create a DynamoDB table
2. Try to create an IAM role without having correct permissions
3. Retry step 2, just in case it's an intermittent failure
4. Rollback: Delete the created DynamoDB table
5. Re-raise the original exception

It is now up to the user to fix the incorrect permissions, and re-run the script.  
