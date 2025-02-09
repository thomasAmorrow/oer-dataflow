from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import zipfile
import os
import subprocess
import logging
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def download_and_unzip(url, extract_to):
    zip_path = "/tmp/gebco_2024.zip"
    response = requests.get(url)
    with open(zip_path, "wb") as f:
        f.write(response.content)
    
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    
    os.remove(zip_path)

def create_schema(schema_name, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()
    cursor.close()
    conn.close()

def convert_netcdf_to_geotiff(folder_path, schema_name, table_name):
    nc_files = [f for f in os.listdir(folder_path) if f.endswith(".nc")]
    if not nc_files:
        raise FileNotFoundError("No NetCDF file found in the extracted folder")
    
    # Get the correct file name (assuming the file is named GEBCO_2024_TID.nc after extraction)
    nc_file_path = os.path.join(folder_path, nc_files[0])
    geotiff_path = os.path.join(folder_path, f"{table_name}.tif")
    
    # Use gdal_translate to convert NetCDF to GeoTIFF
    gdal_translate_cmd = f"gdal_translate -of GTiff {nc_file_path} {geotiff_path}"
    
    result = subprocess.run(gdal_translate_cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"gdal_translate failed with error: {result.stderr}")
        raise RuntimeError(f"gdal_translate failed: {result.stderr}")
    
    logging.info(f"gdal_translate output: {result.stdout}")
    
    return geotiff_path

def convert_geotiff_to_sql(geotiff_path, schema_name, table_name):
    """Convert GeoTIFF to SQL using raster2pgsql"""
    logging.info(f"Converting GeoTIFF {geotiff_path} to SQL")

    # Generate the SQL file using raster2pgsql
    sql_file_path = "/tmp/gebco_2024.sql"
    raster2pgsql_cmd = f"raster2pgsql -s 4326 -I -t 1000x1000 -C {geotiff_path} {schema_name}.{table_name} > {sql_file_path}"
    result = subprocess.run(raster2pgsql_cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        logging.error(f"raster2pgsql failed with error: {result.stderr}")
        raise RuntimeError(f"raster2pgsql failed: {result.stderr}")

    logging.info(f"raster2pgsql output: {result.stdout}")
    
    return sql_file_path

def load_sql_to_postgis(sql_file_path, postgres_conn_id):
    """Load a SQL file into PostGIS using psql command line tool with password handling."""
    logging.info(f"Loading SQL file {sql_file_path} into PostGIS using psql")

    # Get Postgres connection details from the hook
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()

    host = conn.info.host
    user = conn.info.user
    dbname = conn.info.dbname
    password = conn.info.password  # Get the password

    # Set password via PGPASSWORD environment variable
    env = os.environ.copy()
    env["PGPASSWORD"] = password

    # Build the psql command
    psql_command = f"psql -h {host} -U {user} -d {dbname} -f {sql_file_path}"

    try:
        # Execute the command with environment variable set
        result = subprocess.run(psql_command, shell=True, env=env, capture_output=True, text=True)

        if result.returncode != 0:
            logging.error(f"psql failed with error: {result.stderr}")
            raise RuntimeError(f"psql failed: {result.stderr}")

        logging.info(f"psql output: {result.stdout}")
    except Exception as e:
        logging.error(f"Error executing psql command: {e}")
        raise
    finally:
        conn.close()

    logging.info("SQL file loaded successfully.")

dag = DAG(
    'dataset_ETL_GEBCO_netcdf_to_pgsql',
    default_args=default_args,
    description='DAG to download, unzip, create schema, convert NetCDF to GeoTIFF, convert GeoTIFF to SQL, and upload to PostGIS',
    schedule_interval=None,  # Run every 180 days
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

netcdf_url = "https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024_tid/zip/"
extract_folder = "/tmp/"
schema_name = "bathymetry"
table_name = "gebco_2024"
postgres_conn_id = "oceexp-db"

download_unzip_task = PythonOperator(
    task_id='download_and_unzip',
    python_callable=download_and_unzip,
    op_kwargs={'url': netcdf_url, 'extract_to': extract_folder},
    dag=dag,
)

create_schema_task = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    op_kwargs={'schema_name': schema_name, 'postgres_conn_id': postgres_conn_id},
    dag=dag,
)

convert_netcdf_to_geotiff_task = PythonOperator(
    task_id='convert_netcdf_to_geotiff',
    python_callable=convert_netcdf_to_geotiff,
    op_kwargs={'folder_path': extract_folder, 'schema_name': schema_name, 'table_name': table_name},
    dag=dag,
)

convert_geotiff_to_sql_task = PythonOperator(
    task_id='convert_geotiff_to_sql',
    python_callable=convert_geotiff_to_sql,
    op_kwargs={
        'geotiff_path': f"{extract_folder}/gebco_2024.tif",
        'schema_name': schema_name,
        'table_name': table_name
    },
    dag=dag,
)

load_sql_task = PythonOperator(
    task_id='load_sql_to_postgis',
    python_callable=load_sql_to_postgis,
    op_kwargs={
        'sql_file_path': "/tmp/gebco_2024.sql",
        'postgres_conn_id': postgres_conn_id
    },
    dag=dag,
)

download_unzip_task >> create_schema_task >> convert_netcdf_to_geotiff_task >> convert_geotiff_to_sql_task >> load_sql_task
