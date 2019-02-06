#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from query import query, destination

### GBQ configuration
config_file_path=os.path.expanduser('~/.confluentR.config')

with open(config_file_path) as f:
    my_config = f.readline()[:-1]
parsed_config = json.loads(my_config)
print (parsed_config)

user=parsed_config["user"]
bq_credentials=parsed_config["bq_credentials"]
bq_project = parsed_config["bq_project"]
bq_dataset = parsed_config["bq_dataset"]

### establish query client
service_account_file_path =os.path.expanduser(bq_credentials)
with open(service_account_file_path) as source:
    info = json.load(source)

bq_credentials = service_account.Credentials.from_service_account_info(info)
project_id = bq_project
client = bigquery.Client(credentials=bq_credentials, project=project_id)
print ("Conection setup complete")

def update_table(query, destination):
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    table_ref = client.dataset("sfdc").table(destination)
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_TRUNCATE"
    query_job = client.query(query, job_config = job_config)
    query_job.result()
    print("Query results loaded to table {}".format(table_ref.path))

objects = ["opportunity", "account", "opportunity line item", "campaign", "lead", "pricebook entry", "product2", "user"]
for obj in objects:
    update_table(query[obj], destination[obj])