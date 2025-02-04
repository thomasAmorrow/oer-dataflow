from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from ftplib import FTP
import os
import csv
import chardet
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import Integer, Float, String, Boolean, Date
import logging

# Define file paths
INPUT_CSV_PATH = "/tmp/DSCRTP_NatDB.csv"
OUTPUT_CSV_PATH = "/tmp/DSCRTP_NatDB_cleaned.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def download_csv_ftp(ftp_host, ftp_directory, ftp_filename, output_path):
    """Download a CSV file from an FTP server and save it locally."""
    logging.info("Starting CSV download from FTP")
    with FTP(ftp_host) as ftp:
        ftp.login()
        ftp.cwd(ftp_directory)
        with open(output_path, 'wb') as f:
            ftp.retrbinary(f'RETR {ftp_filename}', f.write)
    logging.info(f"CSV downloaded and saved to {output_path}")

def detect_encoding(file_path, num_bytes=10000):
    """Detect file encoding using a limited read size."""
    with open(file_path, 'rb') as f:
        raw_data = f.read(num_bytes)
    encoding = chardet.detect(raw_data)['encoding']
    logging.info(f"Detected encoding: {encoding}")
    return encoding

def clean_csv(input_path, output_path):
    """Clean CSV file and save with UTF-8 encoding."""
    logging.info("Starting CSV cleaning process")
    file_encoding = detect_encoding(input_path)
    with open(input_path, 'r', encoding=file_encoding, errors='replace') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            writer.writerow(row)
    logging.info(f"CSV file cleaned and saved to {output_path}")

def load_csv_to_postgres(csv_path, schema_path, dataframe_chunk_size=200000, sql_chunk_size=1000):
    logging.info("Starting data load to Postgres")
    schema_df = pd.read_csv(schema_path)
    expected_headers = [header.strip().lower() for header in schema_df.iloc[:, 0].tolist()]
    data_types = schema_df.iloc[:, 4].tolist()
    
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    engine = create_engine(postgres_hook.get_uri())
    metadata = MetaData()

    type_mapping = {'Logical': Integer, 'Character': String, 'Integer': Integer,
                    'Factor': String, 'Float': Float, 'Date': String}
    columns = [Column(col, type_mapping.get(dtype, String)) for col, dtype in zip(expected_headers, data_types)]
    
    table = Table('dscrtp', metadata, *columns)
    with engine.connect() as connection:
        logging.info("Dropping existing table if it exists")
        connection.execute("DROP TABLE IF EXISTS dscrtp")
        logging.info("Creating new table")
        metadata.create_all(engine)
    
    with engine.connect() as connection:
        logging.info("Starting batch data load")
        for i, df_chunk in enumerate(pd.read_csv(csv_path, chunksize=dataframe_chunk_size, low_memory=False)):
            df_chunk = df_chunk.iloc[:, 3:]
            df_chunk.columns = [col.strip().lower() for col in df_chunk.columns]
            logging.info(f"Processing batch {i + 1}")
            
            for col, dtype in zip(expected_headers, data_types):
                if dtype in ['Integer', 'Float'] and col in df_chunk.columns:
                    df_chunk[col] = pd.to_numeric(df_chunk[col], errors='coerce')
            
            for j in range(0, len(df_chunk), sql_chunk_size):
                chunk = df_chunk.iloc[j:j + sql_chunk_size]
                chunk.to_sql('dscrtp', connection, if_exists='append', index=False, method='multi', chunksize=sql_chunk_size)
        logging.info("Data loading complete")


# Define the DAG
with DAG(
    dag_id='dataset_ETL_DSCRTP_csv_to_SQL',
    default_args=default_args,
    description='A DAG to download a CSV from an FTP server',
    schedule_interval=timedelta(days=180),  # Run every 180 days
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['csv', 'download', 'ftp','sql'],
) as dag:

    # Define task to download the first CSV via FTP
    download_csv_data = PythonOperator(
        task_id='dataset_get_ftp_DSCRTP_csv_data',
        python_callable=download_csv_ftp,
        op_kwargs={
            'ftp_host': 'ftp-oceans.ncei.noaa.gov',
            'ftp_directory': 'nodc/archive/arc0087/0145037/6.6/data/0-data/',
            'ftp_filename': 'DSCRTP_NatDB_20241022-1.csv',
            'output_path': '/tmp/DSCRTP_NatDB.csv',
        },
    )

    # Define task to download the second CSV via FTP
    download_csv_schema = PythonOperator(
        task_id='dataset_get_ftp_DSCRTP_csv_schema',
        python_callable=download_csv_ftp,
        op_kwargs={
            'ftp_host': 'ftp-oceans.ncei.noaa.gov',
            'ftp_directory': 'nodc/archive/arc0087/0145037/6.6/data/0-data/',
            'ftp_filename': '2022_DSCRTP_National_Database_Schema.csv',
            'output_path': '/tmp/DSCRTP_Schema.csv',
        },
    )

    # Task to clean the CSV
    clean_csv_task = PythonOperator(
        task_id='clean_csv_task',
        python_callable=clean_csv,
        op_kwargs={
            'input_path': INPUT_CSV_PATH,
            'output_path': OUTPUT_CSV_PATH,
        },
    )

    # Define the task
    load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'csv_path': '/tmp/DSCRTP_NatDB_cleaned.csv',
        'schema_path': '/tmp/DSCRTP_Schema.csv',
        'dataframe_chunk_size': 200000,
        'sql_chunk_size': 1000
    },
    dag=dag
)

    # Task sequence: First download, then the second one
    download_csv_data >> download_csv_schema >> clean_csv_task >> load_csv_task
