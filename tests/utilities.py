from time import sleep


def delete_tables(dynamodb, names):
    for name in names:
        delete_table(dynamodb, name)


def delete_table(dynamodb, name):
    try:
        dynamodb.delete_table(TableName=name)
        while True:
            dynamodb.describe_table(TableName=name)
            sleep(1)
    except dynamodb.exceptions.ResourceNotFoundException:
        # Table might not exist (anymore)
        pass


def delete_policies(iam, arns):
    for arn in arns:
        iam.delete_policy(PolicyArn=arn)


def delete_roles(iam, names):
    for name in names:
        iam.delete_role(RoleName=name)


def detach_role_policies(iam, role_policies):
    for role, policy in role_policies:
        iam.detach_role_policy(RoleName=role, PolicyArn=policy)


def delete_mappings(lmbda, mappings):
    for uuid in mappings:
        lmbda.delete_event_source_mapping(UUID=uuid)


def delete_functions(lmbda, functions):
    for name in functions:
        lmbda.delete_function(FunctionName=name)
