# BUCKET_NAME tentacles-cse-ds-workshop
# DATASET_ID ga_data
# TABLE_ID user_segments_exported
# FILENAME API[MP]_config[test].csv

from google.cloud import bigquery
from google.cloud import storage

import base64
import os
import csv
import json

# parsing the function parameters
project = os.environ['GCP_PROJECT']
bucket_name = os.environ['BUCKET_NAME']
dataset_id = os.environ['DATASET_ID']
table_id = os.environ['TABLE_ID']
source_file_name = "ga_user_import.json"
destination_filename = os.environ['FILENAME']
destination_uri = "gs://{}/{}".format(bucket_name, source_file_name)

# Initialise a BigQuery client
client = bigquery.Client()
dataset_ref = client.dataset(dataset_id, project=project)
table_ref = dataset_ref.table(table_id)

def main(event, context):
    # Part 1 Export Data from BigQuery to Storage

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    
    job_config = bigquery.job.ExtractJobConfig(print_header=False)

    extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US", job_config=job_config
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

    # Part 2 convert csv to json
    output_data = []
    NUM_DUPLICATES = 10000      # artificially boost the number of messages

    # Initialise a client
    storage_client = storage.Client(project)
    bucket = storage_client.get_bucket(bucket_name)
    source_blob = bucket.blob(source_file_name)

    csv_file_string = source_blob.download_as_string().decode("utf-8")

    for row in csv_file_string.split('\n'):
        values = row.split(',')
        if len(values) == 2:
            cid = values[0]
            cd2 = values[1]

            for i in range(NUM_DUPLICATES):
                output_data.append('{{"cid": "{}", "cd2": "{}"}}\n'.format(cid, cd2))
                
    destination_blob = bucket.blob('outbound' + '/' + destination_filename)
    destination_blob.upload_from_string('\n'.join(output_data))         # upload data from string

    source_blob.delete()

    print("Source Blob {} deleted.".format(source_blob))