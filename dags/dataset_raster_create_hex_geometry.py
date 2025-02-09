from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import h3ronpy.raster
import rasterio
import pandas as pd
import os
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to process raster to H3

def raster_to_h3(geotiff_path, h3_resolution, output_csv):
    with rasterio.open(geotiff_path) as src:
        affine = src.transform
        array = src.read(1)  # Read the first band
        nodata_value = src.nodata

    df = h3ronpy.raster.raster_to_geodataframe(
        array=array, transform=affine, resolution=h3_resolution, nodata_value=nodata_value
    )

    df.to_csv(output_csv, index=False)
    return output_csv

# Function to load H3 data to PostgreSQL
def load_h3_to_postgis(csv_path, table_name, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id)
    engine = create_engine(pg_hook.get_uri())

    df = pd.read_csv(csv_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# DAG Definition
dag = DAG(
    'gebco_raster_to_h3',
    default_args=default_args,
    description='Convert GEBCO raster to H3 hexagons and load into PostGIS',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

task_raster_to_h3 = PythonOperator(
    task_id='convert_raster_to_h3',
    python_callable=raster_to_h3,
    op_kwargs={
        'geotiff_path': '/tmp/gebco_2024.tif',
        'h3_resolution': 5,
        'output_csv': '/tmp/gebco_h3.csv',
    },
    dag=dag,
)

task_load_h3 = PythonOperator(
    task_id='load_h3_to_postgis',
    python_callable=load_h3_to_postgis,
    op_kwargs={
        'csv_path': '/tmp/gebco_h3.csv',
        'table_name': 'gebco_h3',
        'postgres_conn_id': 'oceexp-db',
    },
    dag=dag,
)

task_raster_to_h3 >> task_load_h3


______________ OR _______________


from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_raster_to_h3(postgres_conn_id, schema_name, raster_table, h3_table, h3_resolution):
    """Processes the raster inside PostgreSQL to generate H3 hexagons."""
    
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    logging.info(f"Dropping existing table {schema_name}.{h3_table} if it exists...")
    cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{h3_table};")

    logging.info(f"Creating new H3 table {schema_name}.{h3_table}...")
    cursor.execute(f"""
        CREATE TABLE {schema_name}.{h3_table} (
            h3_index TEXT PRIMARY KEY,
            avg_value FLOAT
        );
    """)

    logging.info("Processing raster data into H3 hexagons...")
    sql_query = f"""
        INSERT INTO {schema_name}.{h3_table} (h3_index, avg_value)
        SELECT 
            h3.ST_H3FromGeo(ST_Centroid((ST_PixelAsPolygon(rast, 1))).geom, {h3_resolution}) AS h3_index,
            AVG(ST_Value(rast, 1, (ST_PixelAsPolygon(rast, 1)).geom)) AS avg_value
        FROM {schema_name}.{raster_table}
        GROUP BY h3_index;
    """

    cursor.execute(sql_query)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Raster processing to H3 complete.")

dag = DAG(
    'dataset_raster_to_h3_pgsql',
    default_args=default_args,
    description='Process GEBCO raster inside PostgreSQL to generate H3 hexagons',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

schema_name = "bathymetry"
raster_table = "gebco_2024"
h3_table = "gebco_h3"
h3_resolution = 6  # Adjust as needed
postgres_conn_id = "oceexp-db"

process_raster_task = PythonOperator(
    task_id='process_raster_to_h3_pgsql',
    python_callable=process_raster_to_h3,
    op_kwargs={
        'postgres_conn_id': postgres_conn_id,
        'schema_name': schema_name,
        'raster_table': raster_table,
        'h3_table': h3_table,
        'h3_resolution': h3_resolution,
    },
    dag=dag,
)

