#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os

def setup_gbp(config_path):
    """Set up GBQ connection
    :param config_path: path to json file which stores BQ's credentials
    :return: client object
    """
    config_file_path=os.path.expanduser(config_path)

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
    
    scopes = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery']

    bq_credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    project_id = bq_project

    client = bigquery.Client(credentials=bq_credentials, project=project_id)
    print ("Conection setup complete")
    return client

def query_to_bq(client, query, destination):
    """Write the query result into a Google BigQuery table.
    :param client: bigquery client
    :param query: the query
    :param destination_table: Name of table to be written, in the form of dataset.tablename.
    :return: None
    """
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    dest = destination.split('.')
    table_ref = client.dataset(dest[0]).table(dest[1])
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_TRUNCATE"
    query_job = client.query(query, job_config = job_config)
    query_job.result()
    print("Query results loaded to table {}".format(table_ref.path))

def query_append_bq(client, query, destination):
    """Append the query result into a existing BigQuery table.
    :param client: bigquery client
    :param query: the query
    :param destination_table: Name of table to be written, in the form of dataset.tablename.
    :return: None
    """
    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    dest = destination.split('.')
    table_ref = client.dataset(dest[0]).table(dest[1])
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_APPEND"
    query_job = client.query(query, job_config = job_config)
    query_job.result()
    print("Query results loaded to table {}".format(table_ref.path))

def load_data_from_file(client, dataset_id, table_id, source_file_name):
    """Create a table in BigQuery from a local file
    :param client: bigquery client
    :param dataset_id: dataset name for destination table
    :param table_id: destination table name
    :param source_file_name: file name
    :return: None
    """
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'text/csv'
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.autodetect=True
        job_config.skip_leading_rows = 1
        job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))


def df_to_bq(client, df, destination_table):
    """Write a DataFrame to a Google BigQuery table.
    :param client: bigquery client
    :param df: the dataframe name to be copied
    :param destination_table: Name of table to be written, in the form dataset.tablename.
    :return: None
    """
    file_name = 'file.csv'
    dest = destination_table.split('.')
    df.to_csv(file_name, sep = ",", index = False, header = True)
    load_data_from_file(client, dest[0], dest[1], file_name)

