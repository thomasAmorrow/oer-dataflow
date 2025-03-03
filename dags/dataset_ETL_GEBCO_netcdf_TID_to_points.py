from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import zipfile
import os
import tempfile
import subprocess
import logging
import xarray as xr
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

netcdf_url = "https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024_tid/zip/"
schema_name = "public"
table_name = "gebco_2024_TID"
postgres_conn_id = "oceexp-db"

with DAG(
    'dataset_ETL_GEBCO_netcdf_TID_to_points',
    default_args=default_args,
    description='Download, unzip, process, and load GEBCO NetCDF as point data into PostGIS',
    schedule_interval=None,  
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    @task
    def download_and_unzip(url):
        """Download the ZIP file, extract contents, and return the extracted directory path."""
        temp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(temp_dir, "gebco_2024.zip")

        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        os.remove(zip_path)  # Cleanup zip file
        logging.info(f"Extracted files to: {temp_dir}")
        return temp_dir

    @task
    def convert_netcdf_to_points(folder_path):
        """Convert the NetCDF file to a DataFrame of x, y, TID points."""
        nc_files = [f for f in os.listdir(folder_path) if f.endswith(".nc")]
        if not nc_files:
            raise FileNotFoundError("No NetCDF file found after extraction.")

        nc_file_path = os.path.join(folder_path, nc_files[0])
        ds = xr.open_dataset(nc_file_path, engine="netcdf4")  # Explicitly specify engine
        var_name = list(ds.data_vars.keys())[0]  # Assuming the first variable is what we need

        df = ds.to_dataframe().reset_index()
        df = df.rename(columns={var_name: "TID"})
        df = df.dropna()
        csv_path = os.path.join(folder_path, "gebco_2024_TID.csv")
        df.to_csv(csv_path, index=False)

        logging.info(f"Converted NetCDF to CSV with points: {csv_path}")
        return csv_path

    @task
    def load_points_to_postgis(csv_path, schema_name, table_name, postgres_conn_id):
        """Load the CSV file into PostGIS as a table with x, y, TID columns."""
        pg_hook = PostgresHook(postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                id SERIAL PRIMARY KEY,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                TID DOUBLE PRECISION
            );
        """
        )

        with open(csv_path, 'r') as f:
            next(f)  # Skip header row
            cursor.copy_expert(f"""
                COPY {schema_name}.{table_name}(longitude, latitude, TID)
                FROM STDIN WITH CSV DELIMITER ','
            """, f)
        
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"CSV file {csv_path} successfully loaded into {schema_name}.{table_name}")

    # DAG task dependencies
    extracted_folder = download_and_unzip(netcdf_url)
    csv_path = convert_netcdf_to_points(extracted_folder)
    load_points_to_postgis(csv_path, schema_name, table_name, postgres_conn_id)
