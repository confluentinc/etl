# etl
Code for ETL data pipelines 

** Authenticate BigQuery
 - scp BQ credentials to VM src_bash{gcloud compute scp ~/.confluentR.config   --project [project_id] --zone [zone] [name]:~/}

** Set up python3 environment in VM and install packages
 - Install virtualenv if you don't have it yet:sudo pip install virtualenv 
 - Create virtual env: virtualenv -p /usr/bin/python3 env
 - Activate virtual env: . ~/etl/env/bin/activate
 - Install google cloud package: pip install google-cloud-bigquery

** Create a cron job to get it scheduled
 - chmod 755 ~/etl/dedup_stitch_sfdc.py
 - crontab -e
 - Add this line to have it run at 12 p.m. everyday: 0 12 * * * . ~/etl/exec.sh