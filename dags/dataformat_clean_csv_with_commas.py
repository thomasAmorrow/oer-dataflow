from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import chardet
import os

# Define file paths (replace with your actual paths)
INPUT_CSV_PATH = "/tmp/DSCRTP_NatDB.csv"
OUTPUT_CSV_PATH = "/tmp/DSCRTP_NatDB_cleaned.csv"

# Function to clean the CSV
def clean_csv(input_path, output_path):
    # Detect the file encoding using chardet
    with open(input_path, 'rb') as infile:
        raw_data = infile.read()
        result = chardet.detect(raw_data)
        file_encoding = result['encoding']
    
    # Open the file with the detected encoding
    with open(input_path, 'r', encoding=file_encoding, errors='replace') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            writer.writerow(row)

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Define the DAG
with DAG(
    dag_id='dataformat_clean_csv_with_commas',
    default_args=default_args,
    description='A DAG to clean CSV files with inconsistent commas',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    # Task to clean the CSV
    clean_csv_task = PythonOperator(
        task_id='clean_csv_task',
        python_callable=clean_csv,
        op_kwargs={
            'input_path': INPUT_CSV_PATH,
            'output_path': OUTPUT_CSV_PATH,
        },
    )

    clean_csv_task
