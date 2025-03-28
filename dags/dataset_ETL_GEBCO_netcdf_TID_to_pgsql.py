from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
import tempfile
import subprocess
import logging
import netCDF4
from netCDF4 import Dataset
import psycopg2
from psycopg2 import sql

def download_and_unzip(url):
    """Download the ZIP file, extract contents, and return the extracted directory path."""
    zip_path = "/mnt/data/gebco_2024.zip"

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall("/mnt/data/")

    #os.remove(zip_path)  # Cleanup zip file
    logging.info("Extracted files to: /mnt/data/")
    #return temp_dir


def netcdf_to_points():
    file_path = "/mnt/data/GEBCO_2024_TID.nc"

    logging.info("Reading netcdf file.")
    
    data = Dataset(file_path, 'r')
    latitudes = data.variables['lat'][:]
    longitudes = data.variables['lon'][:]
    TIDs = data.variables['tid'][:]

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    
    # SQL statements for creating extensions and table
    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

        DROP TABLE IF EXISTS gebco_tid;
        
        CREATE TABLE gebco_tid (
            latitude DECIMAL,
            longitude DECIMAL,
            tid NUMERIC
        );
    """
    
    # execute SQL statement to create table
    try:
        logging.info("Executing SQL statements to create table...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("Table created successfully!")

        # Insert latitudes, longitudes, and TIDs
        insert_query = """
            INSERT INTO gebco_tid (latitude, longitude, tid) 
            VALUES (%s, %s, %s)
        """
        
        # Prepare data for insertion (ensure correct types)
        data_to_insert = [
            (lat, lon, int(tid))  # Ensure TID is an integer
            for lat, lon, tid in zip(latitudes, longitudes, TIDs)
        ]

        # Execute the insert statements
        logging.info("Inserting data into the table...")
        pg_hook.insert_rows(table="gebco_tid", rows=data_to_insert, commit_every=1000)
        logging.info("Data insertion completed successfully!")

    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

netcdf_url = "https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024_tid/zip/"
table_name = "gebco_2024"
postgres_conn_id = "oceexp-db"

dag = DAG(
    'dataset_ETL_GEBCO_netcdf_TID_to_pgsql',
    default_args=default_args,
    description='Download, unzip, process, and load GEBCO NetCDF raster into PostGIS',
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

download_and_unzip = PythonOperator(
    task_id='download_and_unzip',
    python_callable=download_and_unzip,
    op_args=[netcdf_url],
    dag=dag
)

netcdf_to_XYTID_points = PythonOperator(
    task_id='netcdf_to_XYTID_points',
    python_callable=netcdf_to_points,
    dag=dag
)

# DAG task dependencies
download_and_unzip >> netcdf_to_XYTID_points