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
    zip_path = "/tmp/gmrt_swath_polygons.zip"
    response = requests.get(url)
    with open(zip_path, "wb") as f:
        f.write(response.content)
    
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Disable deletion for debugging
    # os.remove(zip_path)

def create_schema(schema_name, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()
    cursor.close()
    conn.close()

def convert_shapefile_to_sql(folder_path, schema_name, table_name):
    shp_files = [f for f in os.listdir(folder_path) if f.endswith(".shp")]
    if not shp_files:
        raise FileNotFoundError("No shapefile found in the extracted folder")
    
    shp_file_path = os.path.join(folder_path, shp_files[0])
    sql_file_path = os.path.join(folder_path, f"{table_name}.sql")
    
    shp2pgsql_cmd = f"shp2pgsql -I -s 4326 {shp_file_path} {schema_name}.{table_name} > {sql_file_path}"
    subprocess.run(shp2pgsql_cmd, shell=True, check=True)
    
    return sql_file_path

def load_sql_to_postgis(sql_file_path, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id)
    psql_cmd = f"PGPASSWORD={pg_hook.password} psql -h {pg_hook.host} -U {pg_hook.user} -d {pg_hook.schema} -f {sql_file_path}"
    subprocess.run(psql_cmd, shell=True, check=True)

dag = DAG(
    'dataset_ETL_GMRT_shp_to_pgsql',
    default_args=default_args,
    description='DAG to download, unzip, create schema, convert shapefile, and upload SQL to PostGIS',
    schedule_interval=timedelta(days=180),  # Run every 180 days
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

shapefile_url = "https://www.gmrt.org/shapefiles/gmrt_swath_polygons.zip"
extract_folder = "/tmp/"
schema_name = "spatial_data"
table_name = "gmrt_swath"
postgres_conn_id = "oceexp-db"

download_unzip_task = PythonOperator(
    task_id='download_and_unzip',
    python_callable=download_and_unzip,
    op_kwargs={'url': shapefile_url, 'extract_to': extract_folder},
    dag=dag,
)

create_schema_task = PythonOperator(
    task_id='create_schema',
    python_callable=create_schema,
    op_kwargs={'schema_name': schema_name, 'postgres_conn_id': postgres_conn_id},
    dag=dag,
)

convert_shp_to_sql_task = PythonOperator(
    task_id='convert_shapefile_to_sql',
    python_callable=convert_shapefile_to_sql,
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

download_unzip_task >> create_schema_task >> convert_shp_to_sql_task >> load_sql_task
