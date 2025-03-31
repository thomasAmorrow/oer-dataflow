import time
import logging
import csv
import wget
import zipfile
import os
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



def fetch_OBIS_table():
    filename = '/mnt/data/obis.zip'
    url = 'https://obis-datasets.s3.us-east-1.amazonaws.com/exports/obis_20250318_parquet.zip'
    filename = wget.download(url)

    if os.path.exists(filename):
        logging.info("Download successful!")
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall("/mnt/data/")


# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

fetch_OBIS_query_table = PythonOperator(
    task_id='fetch_OBIS_query_table',
    python_callable=fetch_OBIS_table,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_OBIS_query_table >> load_OBIS_table >> assign_hexes_to_OBIS