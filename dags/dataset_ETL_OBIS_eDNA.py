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
    #filename = '/mnt/data/obis.zip'
    url = 'https://obis-datasets.s3.us-east-1.amazonaws.com/exports/obis_20250318_tsv.zip'
    filename = wget.download(url)

    if os.path.exists(filename):
        logging.info("Download successful!")
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extract("occurrence.tsv","/mnt/data/")

def load_OBIS_table(input_file):
    # Check if the file exists before processing
    if not os.path.exists(input_file):
        logging.info(f"Input file {input_file} not found.")
        #raise Exception(f"Input file {input_file} not found.")

    output_file = '/mnt/data/cleaned_OBIS.csv'
    
    # clean tsv
    with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
        open(output_file, 'w', newline='', encoding='utf-8') as outfile:

        # Create a CSV reader and writer
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Process each row
        for row in reader:
            try:    
                # Check if the number of fields is 50
                if len(row) == 282:
                    # Check if any of columns 42 (index 41), 43 (index 42), or 45 (index 44) are empty
                    if not row[41] or not row[42] or not row[44]:  # Check if any of these columns are missing
                        continue  # Skip this row if any of the columns are missing

                    # Create a new row 
                    processed_row = [
                        field
                        for index, field in enumerate(row)
                    ]

                    # Write the processed row to the output file
                    writer.writerow(processed_row)

            except csv.Error as e:
                # Handle the error: skip the row with the large field
                logging.info(f"Skipping row due to error: {e}")
                continue  # Skip the current row and proceed to the next one
    
    logging.info("Finished cleaning file, cleanup started...")

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

load_OBIS_table = PythonOperator(
     task_id='load_OBIS_table',
     python_callable=load_OBIS_table('occurrence.tsv'),
     provide_context=True,
    dag=dag
)

# Define task dependencies
#fetch_OBIS_query_table >> load_OBIS_table >> assign_hexes_to_OBIS
fetch_OBIS_query_table >> load_OBIS_table 