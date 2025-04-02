import time
import logging
import csv
import wget
import os
import shutil
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

csv.field_size_limit(sys.maxsize)

def fetch_GLODAP_table():
    url = 'https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0283442/GLODAPv2.2023_Merged_Master_File.csv'
    filename = wget.download(url)

    if os.path.exists(filename):
        logging.info("Download successful!")
        shutil.move("./GLODAPv2.2023_Merged_Master_File.csv", "/mnt/data/GLODAPv2.2023_Merged_Master_File.csv")

        input_file="/mnt/data/GLODAPv2.2023_Merged_Master_File.csv"
        output_file="/mnt/data/GLODAP_cleaned.csv"

        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            # Create a CSV reader and writer
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Process each row
            for row in reader:
                try:    
                    # Check if the number of fields is 50
                    if len(row) == 109:
                        # Create a new row with quotes around most fields, except for fields 22 and 23
                        processed_row = [
                            'NULL' if field == '-9999' else field
                            for index, field in enumerate(row)
                        ]
                        # Write the processed row to the output file
                        writer.writerow(processed_row)
                except csv.Error as e:
                    # Handle the error: skip the row with the large field
                    logging.info(f"Skipping row due to error: {e}")
                    continue  # Skip the current row and proceed to the next one
        
        logging.info("Finished cleaning file, cleanup started...")


def load_GLODAP_table():

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

        DROP TABLE IF EXISTS glodap;

        COPY glodap (
            expocode,
            cruise,
            station,
            region,
            cast,
            year,
            month,
            day,
            hour,
            minute,
            latitude,
            longitude,
            bottomdepth,
            maxsampdepth,
            bottle,
            pressure,
            depth,
            temperature,
            theta,
            salinity,
            salinityf,
            salinityqc,
            sigma0,
            sigma1,
            sigma2,
            sigma3,
            sigma4,
            gamma,
            oxygen,
            oxygenf,
            oxygenqc,
            aou,
            aouf,
            nitrate,
            nitratef,
            nitrateqc,
            nitrite,
            nitritef,
            silicate,
            silicatef,
            silicateqc,
            phosphate,
            phosphatef,
            phosphateqc,
            tco2,
            tco2f,
            tco2qc,
            talk,
            talkf,
            talkqc,
            fco2,
            fco2f,
            fco2temp,
            phts25p0,
            phts25p0f,
            phtsinsitutp,
            phtsinsitutpf,
            phtsqc,
            cfc11,
            pcfc11,
            cfc11f,
            cfc11qc,
            cfc12,
            pcfc12,
            cfc12f,
            cfc12qc,
            cfc113,
            pcfc113,
            cfc113f,
            cfc113qc,
            ccl4,
            pccl4,
            ccl4f,
            ccl4qc,
            sf6,
            psf6,
            sf6f,
            sf6qc,
            c13,
            c13f, 
            c13qc, 
            c14,
            c14f, 
            c14err, 
            h3, 
            h3f, 
            h3err, 
            he3, 
            he3f, 
            he3err, 
            he, 
            hef, 
            heerr, 
            neon,
            neonf,
            neonerr,
            o18,
            o18f, 
            toc,
            tocf, 
            doc,
            docf, 
            don,
            donf, 
            tdn,
            tdnf, 
            chla,
            chlaf, 
            doi
        )
        FROM '/var/lib/postgresql/data/cleaned_NR50.csv'
        WITH (FORMAT csv, HEADER true, QUOTE '"'); 
    """

    try:
        logging.info("Executing SQL statements...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

    logging.info("Cleaning up csv...")
    os.remove("/mnt/data/GLODAPv2.2023_Merged_Master_File.csv")

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
    'dataset_ETL_GLODAP_obs',
    default_args=default_args,
    description='Fetch occurrences from GLODAP, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_clean_GLODAP_table = PythonOperator(
    task_id='fetch_clean_GLODAP_table',
    python_callable=fetch_GLODAP_table,
    provide_context=True,
    dag=dag
)

load_cleaned_GLODAP_table = PythonOperator(
    task_id='load_cleaned_GLODAP_table',
    python_callable=load_GLODAP_table,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_clean_GLODAP_table >> load_cleaned_GLODAP_table