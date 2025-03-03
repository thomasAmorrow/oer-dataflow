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
table_name = "gebco_2024"
postgres_conn_id = "oceexp-db"

with DAG(
    'dataset_ETL_GEBCO_netcdf_TID_to_pgsql',
    default_args=default_args,
    description='Download, unzip, process, and load GEBCO NetCDF raster into PostGIS',
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
    def create_schema(schema_name, postgres_conn_id):
        """Ensure the schema exists in the database."""
        pg_hook = PostgresHook(postgres_conn_id)
        pg_hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        logging.info(f"Schema '{schema_name}' ensured.")

    @task
    def convert_netcdf_to_geotiff(folder_path):
        """Convert the NetCDF file to GeoTIFF using GDAL."""
        nc_files = [f for f in os.listdir(folder_path) if f.endswith(".nc")]
        if not nc_files:
            raise FileNotFoundError("No NetCDF file found after extraction.")

        nc_file_path = os.path.join(folder_path, nc_files[0])
        geotiff_path = os.path.join(folder_path, "gebco_2024.tif")

        gdal_translate_cmd = f"gdal_translate -of GTiff -co COMPRESS=LZW {nc_file_path} {geotiff_path}"
        result = subprocess.run(gdal_translate_cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"gdal_translate failed: {result.stderr}")

        logging.info(f"GeoTIFF created: {geotiff_path}")
        return geotiff_path

    @task
    def load_raster_to_postgis(geotiff_path, schema_name, table_name, postgres_conn_id):
        """Load the GeoTIFF into PostGIS using raster2pgsql and PostgresHook."""
        pg_hook = PostgresHook(postgres_conn_id)

        raster2pgsql_cmd = f"raster2pgsql -s 4326 -I -t 256x256 -C {geotiff_path} {schema_name}.{table_name}"
        result = subprocess.run(raster2pgsql_cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"raster2pgsql failed: {result.stderr}")

        sql_commands = result.stdout.split("\n")
        pg_hook.run(sql_commands)

        logging.info(f"Raster {geotiff_path} successfully loaded into {schema_name}.{table_name}")

    # DAG task dependencies
    extracted_folder = download_and_unzip(netcdf_url)
    create_schema(schema_name, postgres_conn_id) >> extracted_folder

    geotiff_file = convert_netcdf_to_geotiff(extracted_folder)
    load_raster_to_postgis(geotiff_file, schema_name, table_name, postgres_conn_id)
