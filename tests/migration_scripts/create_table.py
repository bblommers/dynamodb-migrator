#!/usr/bin/python

from migrator import dynamodb_migrator


@dynamodb_migrator.version(1)
@dynamodb_migrator.create("first_table")
def v1(created_table):
    assert created_table['TableName'] == 'first_table'
    assert created_table['TableStatus'] == 'ACTIVE'
