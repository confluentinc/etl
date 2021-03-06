#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from query_zendesk import sql
from datetime import datetime
from utility import setup_gbp, query_to_bq, query_append_bq


print(datetime.now())
client = setup_gbp('~/.confluentR.config')

incremental_table = ["change_event", "audit"]

full_table = ["user", "organization", "ticket_metric", "ticket_data", "ticket_priority", "ticket_initial_priority",
			  "ticket_time_spent", "ticket_component", "ticket_cause", "bundle_usage", "ticket_kafka_version",
			  "ticket_java_version", "ticket_operating_system", "ticket", "satisfaction_rating",
			  "ticket_csat", "csat_trend", "zendesk_sfdc_mapping", "organization_metrics", "rep_organization_mapping", "rep_sfdc_account_map"]

# Insert newly appended records from stitch audit table to audit and change_event
for t in incremental_table:
	destination = "zendesk_v." + t
	query_append_bq(client, sql[t], destination)

# Recreate all the flatten tables
for t in full_table:
	destination = "zendesk_v." + t
	query_to_bq(client, sql[t], destination)
