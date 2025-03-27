import os
import wget
import zipfile
import pygbif
import time
import logging
import csv
from pygbif import occurrences as occ
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

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
    if os.path.exists(f"/mnt/data/{occdatakey}.zip"):
        logging.info("Download successful!")
        with zipfile.ZipFile(f"/mnt/data/{occdatakey}", "r") as zip_ref:
            zip_ref.extractall("/mnt/data/")

        logging.info(f"Key successfully identified as {occdatakey}")

        # Input and output file paths
        input_file = f"/mnt/data/{occdatakey}.csv"
        output_file = '/mnt/data/cleaned_NR50.csv'

        # Check if the file exists before processing
        if not os.path.exists(input_file):
            logging.error(f"Input file {input_file} not found.")
            raise Exception(f"Input file {input_file} not found.")
        
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            # Create a CSV reader and writer
            reader = csv.reader(infile, delimiter='\t')
            writer = csv.writer(outfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Process each row
            for row in reader:
                # Check if the number of fields is 50
                if len(row) == 50:
                    # Add quotes to each field and write the row to the output file
                    writer.writerow([f'"{field}"' for field in row])
        
        logging.info("Finished cleaning file!")

    elif retries < 6:
        try:
            logging.info("Downloading occurrence data...")
            occ.download_get(key=occdatakey, path="/mnt/data/")
        except Exception as e:
            retries += 1
            delay = 60 * min(2 ** retries, 32)  # Exponential backoff with a maximum of 32 minutes
            time.sleep(delay)
    else:
        logging.info("Failed download, too many tries")
    

def load_GBIF_table_csv():
    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

        DROP TABLE IF EXISTS gbif_occurrences;
        
        CREATE TABLE gbif_occurrences (
            gbifid TEXT,
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
            decimallatitude TEXT,
            decimallongitude TEXT,
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
        FROM '/var/lib/postgresql/data/cleaned_NR50.csv'
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

def assign_GBIF_hex():
    # revise for higher resolution in production
    sql_statements = """
        ALTER TABLE gbif_occurrences ADD COLUMN location GEOMETRY(point, 4326);
        UPDATE gbif_occurrences SET location = ST_SETSRID(ST_MakePoint(cast(decimallongitude as float), cast(decimallatitude as float)),4326);

        ALTER TABLE gbif_occurrences ADD COLUMN hex_05 H3INDEX;
        UPDATE gbif_occurrences SET hex_05 = H3_LAT_LNG_TO_CELL(location, 5);
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
