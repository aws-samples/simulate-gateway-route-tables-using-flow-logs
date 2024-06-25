#!/usr/bin/env python3
# This sample script generates the CSV required by the main script from a AWS Config JSON export of VPCs.
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0 

import json
import sys
import os
import argparse
from openpyxl import Workbook

EXCLUDE_CIDRs = ('10.123.123.0/32')

xlsx = Workbook()
sheet = xlsx.active

parser = argparse.ArgumentParser()
parser.add_argument('--configjson', required=True, type=argparse.FileType('r'), help='Path to config VPC JSON query results.')
parser.add_argument('--output', required=True, help='Path to Excel file to write to.')

args = parser.parse_args()
# Load it as a python dict
config = json.load(args.configjson)
try:
  results = config['results']
except:
  print(f'Input does not seem to be a AWS Config JSON export. Missing "results" block.')
  sys.exit(1)

# Do not overwrite existing files
if os.path.isfile(args.output):
  print(f'Error: The output file {args.output} already exists.')
  sys.exit(1)


# Print header:
header = [ 'cidr','account_id','name','vpc_id','associate_with','propagate_to' ]
sheet.append(header)

for result in results:
  configuration = result.get('configuration', {})
  cidr = configuration['cidrBlock']
  if cidr in EXCLUDE_CIDRs:
    continue
  account_id = result.get('accountId', '')
  vpc_id = result.get('resourceId', '')
  tags = configuration.get('tags', [])
  vpc_name = ''
  for tag in tags:
    if tag['key'] == 'Name':
      if ',' in vpc_name:
        raise Exception(f'{vpc_name} ({vpc_id}) has a comma in it')
      vpc_name = tag['value']

  # Print the output as a CSV, see header before the for loop:
  row = [ cidr, account_id, vpc_name, vpc_id, "", "" ]
  sheet.append(row)

xlsx.save(args.output)
print(f'Successfully written to {args.output}')

