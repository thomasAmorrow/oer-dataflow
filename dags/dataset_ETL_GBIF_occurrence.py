import os
import wget
import zipfile
import pygbif
import time
import logging
import csv
import sys
from pygbif import occurrences as occ
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

csv.field_size_limit(sys.maxsize)

def fetch_GBIF_table(**kwargs):
    occdatakey, occdatastring = occ.download(
        format='SIMPLE_CSV',
        user="oerdevops",
        pwd="oceanexploration",
        email="oar.oer.devops@noaa.gov",
        #queries=['depth > 200', 'hasGeospatialIssue = FALSE', 'hasCoordinate = TRUE']
        queries=['depth > 200', 'basisOfRecord = MACHINE_OBSERVATION', 'hasGeospatialIssue = FALSE', 'hasCoordinate = TRUE']
    )
    logging.info("Sleep briefly for GBIF compilation to complete...")
    time.sleep(8 * 60)
    logging.info("*yawn... I'm awake, trying to download...")
    retries = 0

    if retries < 6:
        try:
            logging.info("Downloading occurrence data...")
            occ.download_get(key=occdatakey, path="/mnt/bucket")
        except Exception as e:
            retries += 1
            delay = 60 * min(2 ** retries, 32)  # Exponential backoff with a maximum of 32 minutes
            time.sleep(delay)
    else:
        logging.info("Failed download, too many tries")


    logging.info(f"Looking for /mnt/bucket/{occdatakey}.zip...")

    if os.path.exists(f"/mnt/bucket/{occdatakey}.zip"):
        logging.info("Download successful!")
        with zipfile.ZipFile(f"/mnt/bucket/{occdatakey}.zip", "r") as zip_ref:
            zip_ref.extractall("/mnt/bucket/")

        logging.info(f"Key successfully identified as {occdatakey}")

        # Input and output file paths
        input_file = f"/mnt/bucket/{occdatakey}.csv"
        output_file = '/mnt/bucket/cleaned_NR50.csv'

        # Check if the file exists before processing
        if not os.path.exists(input_file):
            logging.info(f"Input file {input_file} not found.")
            #raise Exception(f"Input file {input_file} not found.")
        
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            # Create a CSV reader and writer
            reader = csv.reader(infile, delimiter='\t')
            writer = csv.writer(outfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Process each row
            for row in reader:
                try:    
                    # Check if the number of fields is 50
                    if len(row) == 50:
                        # Create a new row with quotes around most fields, except for fields 22 and 23
                        processed_row = [
                            field  
                            #f'"{field}"' if index not in [21, 22] else field  # Field 22 is index 21, field 23 is index 22
                            for index, field in enumerate(row)
                        ]
                        # Write the processed row to the output file
                        writer.writerow(processed_row)
                except csv.Error as e:
                    # Handle the error: skip the row with the large field
                    logging.info(f"Skipping row due to error: {e}")
                    continue  # Skip the current row and proceed to the next one
        
        logging.info("Finished cleaning file, cleanup started...")

        os.remove(f"/mnt/bucket/{occdatakey}.csv")
        os.remove(f"/mnt/bucket/{occdatakey}.zip")


def load_GBIF_table_csv():
    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

        DROP TABLE IF EXISTS gbif_occurrences;
        
        CREATE TABLE gbif_occurrences (
            gbifid BIGINT PRIMARY KEY,
            datasetkey TEXT,
            occurrenceid TEXT,
            kingdom TEXT,
            phylum TEXT,
            "class" TEXT,
            "order" TEXT,
            family TEXT,
            genus TEXT,
            species TEXT,
            infraspecificepithet TEXT,
            taxonrank TEXT,
            scientificname TEXT,
            verbatimscientificname TEXT,
            verbatimscientificnameauthorsh TEXT,
            countrycode TEXT,
            locality TEXT,
            stateprovince TEXT,
            occurrencestatus TEXT,
            individualcount TEXT,
            publishingorgkey TEXT,
            decimallatitude DECIMAL,
            decimallongitude DECIMAL,
            coordinateuncertaintyinmeters TEXT,
            coordinateprecision TEXT,
            elevation TEXT,
            elevationaccuracy TEXT,
            depth TEXT,
            depthaccuracy TEXT,
            eventdate TEXT,
            day TEXT,
            month TEXT,
            year TEXT,
            taxonkey TEXT,
            specieskey TEXT,
            basisofrecord TEXT,
            institutioncode TEXT,
            collectioncode TEXT,
            catalognumber TEXT,
            recordnumber TEXT,
            identifiedby TEXT,
            dateidentified TEXT,
            license TEXT,
            rightsholder TEXT,
            recordedby TEXT,
            typestatus TEXT,
            establishmentmeans TEXT,
            lastinterpreted TEXT,
            mediatype TEXT,
            issue TEXT
        );

        COPY gbif_occurrences (
            gbifid,
            datasetkey,
            occurrenceid,
            kingdom,
            phylum,
            "class",
            "order",
            family,
            genus,
            species,
            infraspecificepithet,
            taxonrank,
            scientificname,
            verbatimscientificname,
            verbatimscientificnameauthorsh,
            countrycode,
            locality,
            stateprovince,
            occurrencestatus,
            individualcount,
            publishingorgkey,
            decimallatitude,
            decimallongitude,
            coordinateuncertaintyinmeters,
            coordinateprecision,
            elevation,
            elevationaccuracy,
            depth,
            depthaccuracy,
            eventdate,
            day,
            month,
            year,
            taxonkey,
            specieskey,
            basisofrecord,
            institutioncode,
            collectioncode,
            catalognumber,
            recordnumber,
            identifiedby,
            dateidentified,
            license,
            rightsholder,
            recordedby,
            typestatus,
            establishmentmeans,
            lastinterpreted,
            mediatype,
            issue
        )
        FROM '/mnt/bucket/cleaned_NR50.csv'
        WITH (FORMAT csv, HEADER true, DELIMITER E'\t', QUOTE '"'); 
    """

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    try:
        logging.info("Executing SQL statements...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

    logging.info("Cleaning up csv...")
    os.remove("/mnt/bucket/cleaned_NR50.csv")

def assign_GBIF_hex():
    # revise for higher resolution in production
    sql_statements = """
        ALTER TABLE gbif_occurrences ADD COLUMN location GEOMETRY(point, 4326);
        UPDATE gbif_occurrences SET location = ST_SETSRID(ST_MakePoint(cast(decimallongitude as float), cast(decimallatitude as float)),4326);

        ALTER TABLE gbif_occurrences ADD COLUMN hex_06 H3INDEX;
        UPDATE gbif_occurrences SET hex_06 = H3_LAT_LNG_TO_CELL(location, 6);
    """
     # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    try:
        logging.info("Executing SQL statements...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

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
    'dataset_ETL_GBIF_occurrence',
    default_args=default_args,
    description='Fetch occurrences from GBIF, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_GBIF_query_table = PythonOperator(
    task_id='fetch_GBIF_query_table',
    python_callable=fetch_GBIF_table,
    provide_context=True,
    dag=dag
)

load_GBIF_table = PythonOperator(
    task_id='load_GBIF_table',
    python_callable=load_GBIF_table_csv,
    provide_context=True,
    dag=dag
)

assign_hexes_to_GBIF = PythonOperator(
    task_id='assign_hexes_to_GBIF',
    python_callable=assign_GBIF_hex,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_GBIF_query_table >> load_GBIF_table >> assign_hexes_to_GBIF
