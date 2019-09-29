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

