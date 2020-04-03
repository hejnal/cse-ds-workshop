#!/usr/bin/env python

import csv
import json
from google.cloud import storage

output_data = []
NUM_DUPLICATES = 10000

storage_client = storage.Client("cse-ds-workshop")
bucket = storage_client.get_bucket("tentacles-cse-ds-workshop")
source_blob = bucket.blob("user_segments_exported.csv")

csv_file_string = source_blob.download_as_string().decode("utf-8")

for row in csv_file_string.split('\n'):
    values = row.split(',')
    if len(values) == 2:
        cid = values[0]
        cd2 = values[1]

        for i in range(NUM_DUPLICATES):
            output_data.append('{{"cid": "{}", "cd2": "{}"}}\n'.format(cid, cd2))

with open('API[MP]_config[test].json', 'w') as jsonfile:
    for r in output_data:
        jsonfile.write(r)

print("csv file: {}".format(csv_file_string))