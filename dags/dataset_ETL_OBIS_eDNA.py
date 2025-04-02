import time
import logging
import csv
import wget
import zipfile
import os
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

csv.field_size_limit(sys.maxsize)

def fetch_OBIS_table():
    #filename = '/mnt/data/obis.zip'
    url = 'https://obis-datasets.s3.us-east-1.amazonaws.com/exports/obis_20250318_tsv.zip'
    filename = wget.download(url)

    if os.path.exists(filename):
        logging.info("Download successful!")
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extract("occurrence.tsv","/mnt/data/")

def process_chunk(chunk, writer):
    """
    Process a chunk of rows and write to the output file.
    """
    for row in chunk:
        try:
            # Check if the number of fields is 282
            if len(row) == 282:
                # Check if columns 42, 43, or 45 are empty
                if not row[41] or not row[42] or not row[44] or row[44] < 200:
                    continue  # Skip row if any of these columns are empty

                # Process the row (if needed)
                processed_row = row  # You could add further processing here if needed

                # Write the processed row to the output file
                writer.writerow(processed_row)

        except csv.Error as e:
            logging.info(f"Skipping row due to CSV error: {e}")
            continue  # Skip problematic row


def load_OBIS_table(chunk_size=1000):
    input_file = '/mnt/data/occurrence.tsv'
    output_file = '/mnt/data/cleaned_OBIS.csv'
    
    # Check if the file exists before processing
    if not os.path.exists(input_file):
        logging.info(f"Input file {input_file} not found.")
        return

    logging.info(f"Processing file: {input_file}")
    
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile, delimiter='\t')
            writer = csv.writer(outfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Initialize a chunk list to hold a set of rows
            chunk = []
            for row in reader:
                chunk.append(row)
                
                # When the chunk size is reached, process it
                if len(chunk) >= chunk_size:
                    process_chunk(chunk, writer)
                    chunk = []  # Reset chunk after processing

            # Process any remaining rows that didn't fill the last chunk
            if chunk:
                process_chunk(chunk, writer)

        logging.info("Finished cleaning file successfully.")
    
    except Exception as e:
        logging.error(f"Error processing file: {e}")


# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dataset_ETL_OBIS_eDNA',
    default_args=default_args,
    description='Fetch occurrences from OBIS, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_OBIS_query_table = PythonOperator(
    task_id='fetch_OBIS_query_table',
    python_callable=fetch_OBIS_table,
    provide_context=True,
    dag=dag
)

load_cleaned_OBIS_table = PythonOperator(
     task_id='load_cleaned_OBIS_table',
     python_callable=load_OBIS_table,
     provide_context=True,
    dag=dag
)

# Define task dependencies
#fetch_OBIS_query_table >> load_OBIS_table >> assign_hexes_to_OBIS
fetch_OBIS_query_table >> load_cleaned_OBIS_table