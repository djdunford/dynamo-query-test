#!/usr/bin/env python3
"""Example to test paginated query against a global secondary index

AWS credentials need to be configured using "aws configure"
User requires permissions to access DynamoDB to put items and query
Table must already exist in DynamoDB with VIN(string) as the Partition Key
Global Secondary Index must already exist in DynamoDB with make(string) as PartitionKey
and lastupdate(integer) as Sort Key

by Darren Dunford
"""

# import libraries
import csv
import boto3
from boto3.dynamodb.conditions import Key

# parameters
CSV_FILE = './data.csv'
TABLE = 'querytesttable'
INDEX = 'make-lastupdate-index'

if __name__ == '__main__':

    # open DynamoDB connection to table with boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE)

    # read data in from CSV
    items = []
    with open(CSV_FILE) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data = {}
            data['VIN'] = row['VIN']
            data['make'] = row['make']
            data['lastupdate'] = int(row['lastupdate'])
            items.append(data)

    # load data in to table
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item = item)

    # run paginated query - get first page
    response = table.query(
        IndexName = INDEX,
        KeyConditionExpression = Key('make').eq('LR') & Key('lastupdate').gte(20200203180000),
        Limit = 2
    )

    # print response page 1
    print("Page 1 Output: {}".format(response))

    # run paginated query - get second page
    response2 = table.query(
        IndexName = INDEX,
        KeyConditionExpression = Key('make').eq('LR') & Key('lastupdate').gte(20200203180000),
        Limit = 2,
        ExclusiveStartKey = response.get('LastEvaluatedKey')
    )

    # print response page 2
    print("Page 2 Output: {}".format(response2))
