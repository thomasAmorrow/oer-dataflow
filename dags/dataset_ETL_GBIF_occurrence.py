import logging
import geopandas as gpd
import pandas as pd
import h3
from shapely.geometry import shape, Polygon
from shapely.wkt import loads, dumps
from pygbif import occurrences as occ
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



# Function to check if a polygon's coordinates are counter-clockwise
def is_counter_clockwise(polygon_wkt):
    polygon = loads(polygon_wkt)
    return polygon.exterior.is_ccw

# Function to rearrange polygon to counter-clockwise
def rearrange_to_counter_clockwise(polygon_wkt):
    polygon = loads(polygon_wkt)
    if not polygon.exterior.is_ccw:
        polygon = Polygon(list(polygon.exterior.coords)[::-1])
    return dumps(polygon)


def fetch_and_save_occurrences(h3_index, postgres_conn_id='oceexp-db'):
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    logging.info(f"Querying index {h3_index} ...")

    # Get polygon geometry
    polygon = h3.cells_to_geo([h3_index], tight=True)
    polygeo = shape(polygon)

    # If the polygon is not counter-clockwise, rearrange it
    if not polygeo.exterior.is_ccw:
        logging.info(f"Rearranging polygon for H3 index {h3_index} to counter-clockwise")
        polygon_wkt = dumps(polygeo)
        rearranged_wkt = rearrange_to_counter_clockwise(polygon_wkt)
        polygeo = shape(loads(rearranged_wkt))

    # Search for occurrences in the polygon
    critters = occ.search(geometry=polygeo.wkt, limit=10000, depth='200,12000', fields=[
        'latitude', 'longitude', 'depth', 'taxonKey', 'scientificName', 'kingdomKey', 'phylumKey',
        'classKey', 'orderKey', 'familyKey', 'genusKey', 'basisOfRecord'])

    occurrences = []

    # Extract data for each occurrence
    for critter in critters['results']:
        latitude = critter['decimalLatitude']
        longitude = critter['decimalLongitude']
        depth = critter['depth']
        taxonkey = critter['taxonKey']
        scientificname = critter['scientificName']
        kingdom = critter.get('kingdomKey', None)
        phylum = critter.get('phylumKey', None)
        class_key = critter.get('classKey', None)
        order = critter.get('orderKey', None)
        family = critter.get('familyKey', None)
        genus = critter.get('genusKey', None)
        basisofrecord = critter.get('basisOfRecord', None)

        if depth is not None:
            occurrences.append({
                'latitude': latitude,
                'longitude': longitude,
                'depth': depth,
                'taxonkey': taxonkey,
                'scientificname': scientificname,
                'kingdomKey': kingdom,
                'phylumKey': phylum,
                'classKey': class_key,
                'orderKey': order,
                'familyKey': family,
                'genusKey': genus,
                'basisofrecord': basisofrecord,
                'h3_index': h3_index  # Add the h3 index to the occurrences
            })

    # Convert occurrences to DataFrame
    occurrences_df = pd.DataFrame(occurrences)
    if occurrences_df.empty:
        logging.warning(f"No occurrences found for H3 index {h3_index}")
        return

    # Insert occurrences into PostgreSQL
    for _, row in occurrences_df.iterrows():
        cursor.execute("""
            INSERT INTO gbif_occurrences (latitude, longitude, depth, taxonkey, scientificname, kingdomKey,
            phylumKey, classKey, orderKey, familyKey, genusKey, basisofrecord, hex_05)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['latitude'], row['longitude'], row['depth'], row['taxonkey'], row['scientificname'],
                  row['kingdomKey'], row['phylumKey'], row['classKey'], row['orderKey'], row['familyKey'],
                  row['genusKey'], row['basisofrecord'], row['h3_index']))
    conn.commit()
    logging.info(f"Inserted {len(occurrences_df)} occurrences for H3 index {h3_index} into database.")


def fetch_h3_indices_and_create_table(postgres_conn_id='oceexp-db'):
    # Connect to PostgreSQL and fetch the list of H3 indices
    pg_hook = PostgresHook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Fetch all hexagon H3 indices
    cursor.execute("SELECT DISTINCT hex_05 FROM h3_oceans") # it work let's try em all!
    indices = cursor.fetchall()

    # Create the results table if it doesn't exist
    cursor.execute("""
        DROP TABLE IF EXISTS gbif_occurrences;          

        CREATE TABLE IF NOT EXISTS gbif_occurrences (
            id SERIAL PRIMARY KEY,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            depth DOUBLE PRECISION,
            taxonkey TEXT,
            scientificname TEXT,
            kingdomKey TEXT,
            phylumKey TEXT,
            classKey TEXT,
            orderKey TEXT,
            familyKey TEXT,
            genusKey TEXT,
            basisofrecord TEXT,
            hex_05 H3INDEX
        );
    """)
    conn.commit()

    # Return the list of H3 indices
    return [index[0] for index in indices]


# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dataset_ETL_GBIF_occurrence',
    default_args=default_args,
    description='Fetch occurrences for H3 hexagons and save to PostgreSQL',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Task to fetch H3 indices
fetch_indices_task = PythonOperator(
    task_id='fetch_h3_indices',
    python_callable=fetch_h3_indices_and_create_table,
    provide_context=True,
    dag=dag
)

# Task to fetch occurrences for each H3 index
def fetch_occurrences_for_each_hexagon(**kwargs):
    # Get H3 indices from previous task
    indices = kwargs['ti'].xcom_pull(task_ids='fetch_h3_indices')
    
    for h3_index in indices:
        fetch_and_save_occurrences(h3_index)


fetch_occurrences_task = PythonOperator(
    task_id='fetch_occurrences_for_each_hexagon',
    python_callable=fetch_occurrences_for_each_hexagon,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_indices_task >> fetch_occurrences_task
