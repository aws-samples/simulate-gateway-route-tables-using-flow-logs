#!/usr/bin/python3
# Create the SQL to create the Athena tables to analyse the VPC Flow Logs

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import csv
import datetime
import time
import uuid
import boto3

S3_BUCKET = 'fillme'
S3_ATHENA_BUCKET = S3_BUCKET
REGION = 'ap-southeast-2'
START_DATE = datetime.date(2024, 2, 22)
END_DATE = datetime.date(2024, 2, 23)  # Day *after* the last day to process
OUTPUT_FILENAME = 'athena.csv'



def get_days():
    """ Return a date object for every day between START_DATE and END_DATE
    Inclusive of START_DATE, but not END_DATE
    """
    day = START_DATE
    while day < END_DATE:
        yield day
        day += datetime.timedelta(days=1)


def create_request_token():
    """ Create a unique client request token """
    return str(uuid.uuid4())


def run_sql(query_string):
    """ Run a Athena SQL """
    athena_client = boto3.client('athena', region_name=REGION)
    response = athena_client.start_query_execution(
        ClientRequestToken=create_request_token(),
        ResultConfiguration={"OutputLocation": f"s3://{S3_ATHENA_BUCKET}/athena/"},
        QueryString=query_string)
    query_id = response['QueryExecutionId']

    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        status = response['QueryExecution']['Status']['State']
        if status in ('QUEUED', 'RUNNING', 'SCHEDULED'):
            # Still running
            time.sleep(0.1)
        elif status in ('FAILED', 'CANCELLED'):
            if 'AlreadyExistsException' in (
                    response['QueryExecution']['Status']['StateChangeReason']):
                return None  # Already created (partition, probably)
            raise Exception(f"Query failed: {status}")
        elif status == 'SUCCEEDED':
            return (
                response['QueryExecution']['ResultConfiguration']['OutputLocation'])
        else:
            raise Exception(f"Unknown status {status}")

    raise Exception("Code error - should not get here")


def create_table(accounts):
    """ Create the Athena table """
    print("Creating table", file=sys.stderr)
    run_sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS vpc_flow_logs (
              version int,
              account_from_file string,
              interfaceid string,
              sourceaddress string,
              destinationaddress string,
              sourceport int,
              destinationport int,
              protocol int,
              numpackets int,
              numbytes bigint,
              starttime int,
              endtime int,
              action string,
              logstatus string
              )
            PARTITIONED BY (day date, account string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ' '
            LOCATION 's3://{S3_BUCKET}/vpc-flow-logs/AWSLogs/'
            TBLPROPERTIES ('skip.header.line.count'='1')
            """)

    for account in accounts:
        for date in get_days():
            datestr = date.strftime('%Y-%m-%d')
            year = date.strftime('%Y')
            month = date.strftime('%m')
            day = date.strftime('%d')
            print(f"Adding partition for {account} on {datestr}", file=sys.stderr)
            run_sql(f"""
              ALTER TABLE vpc_flow_logs
              ADD PARTITION (day='{datestr}', account='{account}')
              location 's3://{S3_BUCKET}/vpc-flow-logs/AWSLogs/{account}/vpcflowlogs/{REGION}/{year}/{month}/{day}'
            """)


def main():
    """ main """
    s3client = boto3.client('s3', region_name=REGION)
    accounts = []
    for account in s3client.list_objects_v2(
            MaxKeys=10000,
            Bucket=S3_BUCKET,
            Prefix='vpc-flow-logs/AWSLogs/',
            Delimiter='/')['CommonPrefixes']:
        account_id = account['Prefix'].split('/')[-2]
        accounts.append(account_id)

    create_table(accounts)

    print("Running query", file=sys.stderr)
    s3location = run_sql("""
        SELECT
            array_join(slice(split(sourceaddress, '.'), 1, 3), '.') as src,
            array_join(slice(split(destinationaddress, '.'), 1, 3), '.') as dest,
            sum(numpackets) as numpackets,
            count(*) as lines
        FROM "default"."vpc_flow_logs"
        WHERE action = 'ACCEPT'
        AND (
          array_join(slice(split(sourceAddress, '.'), 1, 1), '.') = '10'
          OR (
            array_join(slice(split(sourceAddress, '.'), 1, 1), '.') = '192'
            AND array_join(slice(split(sourceAddress, '.'), 1, 1), '.') = '168'
            )
          OR (
            array_join(slice(split(sourceAddress, '.'), 1, 1), '.') = '172'
            AND cast(array_join(slice(split(sourceAddress, '.'), 1, 1), '.') as integer) >= 16
            AND cast(array_join(slice(split(sourceAddress, '.'), 1, 1), '.') as integer) <= 31
            )
          )
        AND (
          array_join(slice(split(destinationaddress, '.'), 1, 1), '.') = '10'
          OR (
            array_join(slice(split(destinationaddress, '.'), 1, 1), '.') = '192'
            AND array_join(slice(split(destinationaddress, '.'), 1, 1), '.') = '168'
            )
          OR (
            array_join(slice(split(destinationaddress, '.'), 1, 1), '.') = '172'
            AND cast(array_join(slice(split(destinationaddress, '.'), 1, 1), '.') as integer) >= 16
            AND cast(array_join(slice(split(destinationaddress, '.'), 1, 1), '.') as integer) <= 31
            )
          )
        GROUP BY (1, 2)
        """)
    print(f"Retrieving results to {OUTPUT_FILENAME}", file=sys.stderr)
    (_, _, bucket, key) = s3location.split('/', maxsplit=3)
    client = boto3.client('s3')
    with open(OUTPUT_FILENAME, 'wb') as f:
        client.download_fileobj(bucket, key, f)


if __name__ == "__main__":
    main()
