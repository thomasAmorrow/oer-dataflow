from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import zipfile
import os
import subprocess

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

def convert_netcdf_to_sql(folder_path, schema_name, table_name):
    nc_files = [f for f in os.listdir(folder_path) if f.endswith(".nc")]
    if not nc_files:
        raise FileNotFoundError("No NetCDF file found in the extracted folder")

    nc_file_path = os.path.join(folder_path, nc_files[0])
    tif_file_path = os.path.join(folder_path, f"{table_name}.tif")
    sql_file_path = os.path.join(folder_path, f"{table_name}.sql")

    # Convert NetCDF to GeoTIFF using GDAL
    gdal_cmd = f"gdal_translate -of GTiff NETCDF:{nc_file_path}:elevation {tif_file_path}"
    subprocess.run(gdal_cmd, shell=True, check=True)

    # Convert GeoTIFF to SQL using raster2pgsql
    raster2pgsql_cmd = f"raster2pgsql -s 4326 -I -C -M {tif_file_path} {schema_name}.{table_name} > {sql_file_path}"
    subprocess.run(raster2pgsql_cmd, shell=True, check=True)

    return sql_file_path

def load_sql_to_postgis(sql_file_path, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id)
    psql_cmd = f"PGPASSWORD={pg_hook.password} psql -h {pg_hook.host} -U {pg_hook.user} -d {pg_hook.schema} -f {sql_file_path}"
    subprocess.run(psql_cmd, shell=True, check=True)

dag = DAG(
    'dataset_ETL_GEBCO_netcdf_to_pgsql',
    default_args=default_args,
    description='DAG to download, unzip, convert NetCDF to PostGIS, and load into database',
    schedule_interval=timedelta(days=180),
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

convert_netcdf_to_sql_task = PythonOperator(
    task_id='convert_netcdf_to_sql',
    python_callable=convert_netcdf_to_sql,
    op_kwargs={'folder_path': extract_folder, 'schema_name': schema_name, 'table_name': table_name},
    provide_context=True,
    dag=dag,
)

load_sql_task = PythonOperator(
    task_id='load_sql_to_postgis',
    python_callable=load_sql_to_postgis,
    op_kwargs={'sql_file_path': f"{extract_folder}/{table_name}.sql", 'postgres_conn_id': postgres_conn_id},
    dag=dag,
)

download_unzip_task >> create_schema_task >> convert_netcdf_to_sql_task >> load_sql_task
