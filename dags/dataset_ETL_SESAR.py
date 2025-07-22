import time
import logging
import json
import requests
import h3
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

# --- Constants ---
API_HIT_DELAY = 1
PAGE_LIMIT = 500
DB_CONN_ID = "oceexp-db"
METADATA_INSERT_BATCH_SIZE = 500 # Configurable batch size for DB writes

# --- Helper Functions (unchanged) ---
def crosses_antimeridian(boundary):
    longitudes = [lng for lat, lng in boundary]
    return any(
        lng <= -90 and longitudes[i+1] > 90 or
        lng >= 90 and longitudes[i+1] < -90
        for i, lng in enumerate(longitudes[:-1])
    )

def adjust_longitudes_if_crosses_antimeridian(boundary):
    return [[lat, lng + 360 if lng < 0 else lng] for lat, lng in boundary]

def close_polygon(polygon):
    if not polygon:
        raise ValueError("Polygon is empty and cannot be closed.")
    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])
    return polygon

# --- Python Callables for Operators ---

def _get_unprocessed_hexes():
    """
    Fetches H3 hexes from the source table, excluding those already processed.
    This function is called by a PythonOperator.
    """
    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = """
        SELECT hex_03 FROM ega_score_03
        EXCEPT
        SELECT hex_id FROM sesar_processed_hexes;
    """
    records = pg_hook.get_records(sql)
    # get_records returns a list of tuples, e.g., [('8329a1fffffffff',), ...]
    # The PythonOperator will automatically push this returned list to XComs.
    return [row[0] for row in records]

def _fetch_igsns_for_hex(h3_index: str):
    """
    Fetches all IGSNs for a *single* H3 hex and marks it as processed.
    This function is designed to be dynamically mapped.
    """
    logging.info(f"Searching IGSNs for hex {h3_index}...")
    boundary_tuple = h3.cell_to_boundary(h3_index)
    boundary = list(boundary_tuple)

    if crosses_antimeridian(boundary):
        boundary = adjust_longitudes_if_crosses_antimeridian(boundary)

    boundary = close_polygon(boundary)
    geostring = ",".join([f"{lng} {lat}" for lat, lng in boundary])
    
    hex_igsns = []
    page = 1
    while True:
        url = f"https://app.geosamples.org/samples/polygon/{geostring}?limit={PAGE_LIMIT}&page_no={page}&hide_private=1"
        headers = {'Accept': 'application/json'}
        try:
            response = requests.get(url, headers=headers, timeout=60) # Add timeout
            if response.status_code == 500:
                logging.warning(f"Empty or failed polygon for {h3_index}, skipping.")
                break
            response.raise_for_status()
            data = response.json()
            igsns = data.get("igsn_list", [])
            
            if not igsns:
                break
            
            hex_igsns.extend(igsns)
            
            if len(igsns) < PAGE_LIMIT:
                break
            
            page += 1
            time.sleep(API_HIT_DELAY)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed fetching IGSNs for {h3_index}: {e}")
            break
    
    # --- Checkpoint this specific hex as processed ---
    if hex_igsns:
         pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
         pg_hook.run("INSERT INTO sesar_processed_hexes (hex_id) VALUES (%s) ON CONFLICT DO NOTHING;", parameters=(h3_index,))

    return hex_igsns

def _load_sesar_metadata(ti=None):
    """
    Aggregates unique IGSNs from the upstream mapped task and stores their metadata in batches.
    """
    # Pull the list of lists from all the mapped 'fetch_igsns_for_hex' tasks
    igsns_lists = ti.xcom_pull(task_ids='fetch_igsns_for_hex', key='return_value')
    
    unique_igsns = sorted(list({igsn for sublist in igsns_lists for igsn in sublist}))
    total_igsns = len(unique_igsns)
    logging.info(f"Found {total_igsns} unique IGSNs to process.")
    
    if not total_igsns:
        logging.info("No IGSNs to process. Exiting.")
        return

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    # The SQL for table creation is now in its own operator, but we can leave it
    # here as well to be fully self-contained and idempotent.
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS sesar_samples (
            igsn TEXT PRIMARY KEY, material TEXT, classification TEXT, field_name TEXT, 
            description TEXT, collection_method TEXT, collection_method_desc TEXT, 
            size TEXT, geological_age TEXT, geological_unit TEXT, comment TEXT, purpose TEXT, 
            lat_start FLOAT, lat_end FLOAT, lon_start FLOAT, lon_end FLOAT, elevation_start FLOAT, 
            elevation_end FLOAT, physiographic_feature TEXT, physiographic_feature_name TEXT, 
            location_desc TEXT, locality TEXT, locality_desc TEXT, country TEXT, state_province TEXT, 
            county TEXT, city TEXT, field_program TEXT, platform_type TEXT, platform_name TEXT, 
            platform_desc TEXT, launch_type TEXT, launch_platform_name TEXT, launch_id TEXT, 
            collector TEXT, collector_detail TEXT, collection_date_start DATE, collection_date_end DATE, 
            archive TEXT, archive_contact TEXT, orig_archive TEXT, orig_archive_contact TEXT, 
            parent_igsns TEXT[], sibling_igsns TEXT[], child_igsns TEXT[], raw_json JSONB
        );
    """)

    records_to_insert = []
    for idx, igsn in enumerate(unique_igsns):
        # ... [The metadata fetching logic is identical to the previous answer] ...
        # For brevity, this part is condensed. The full logic is in the previous response.
        logging.info(f"Fetching metadata for IGSN {igsn} ({idx + 1}/{total_igsns})")
        url = f"https://app.geosamples.org/sample/igsn/{igsn}"
        headers = {'Accept': 'application/json'}
        try:
            resp = requests.get(url, headers=headers, timeout=60)
            resp.raise_for_status()
            sample = resp.json().get("sample", {})
            record = (
                    sample.get("igsn"), sample.get("material"), sample.get("classification"), sample.get("field_name"), sample.get("description"),
                    sample.get("collection_method"), sample.get("collection_method_desc"), sample.get("size"), sample.get("geological_age"), sample.get("geological_unit"),
                    sample.get("comment"), sample.get("purpose"), sample.get("latitude_start"), sample.get("latitude_end"), sample.get("longitude_start"),
                    sample.get("longitude_end"), sample.get("elevation_start"), sample.get("elevation_end"), sample.get("physiographic_feature"),
                    sample.get("physiographic_feature_name"), sample.get("location_desc"), sample.get("locality"), sample.get("locality_desc"), sample.get("country"),
                    sample.get("state_province"), sample.get("county"), sample.get("city"), sample.get("field_program"), sample.get("platform_type"),
                    sample.get("platform_name"), sample.get("platform_desc"), sample.get("launch_type"), sample.get("launch_platform_name"), sample.get("launch_id"),
                    sample.get("collector"), sample.get("collector_detail"), sample.get("collection_date_start"), sample.get("collection_date_end"), sample.get("archive"),
                    sample.get("archive_contact"), sample.get("orig_archive"), sample.get("orig_archive_contact"), sample.get("parents"), sample.get("siblings"),
                    sample.get("children"), json.dumps(sample)
                )
            records_to_insert.append(record)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Failed fetching metadata for IGSN {igsn}: {e}")
            
        # --- Batched Insert Logic ---
        if len(records_to_insert) >= METADATA_INSERT_BATCH_SIZE or (idx + 1) == total_igsns:
            if records_to_insert:
                logging.info(f"Inserting batch of {len(records_to_insert)} records...")
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        execute_values(cursor, "INSERT INTO sesar_samples VALUES %s ON CONFLICT (igsn) DO NOTHING;", records_to_insert)
                records_to_insert = []
        time.sleep(API_HIT_DELAY)

# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dataset_ETL_SESAR_optimized_traditional',
    default_args=default_args,
    description='Optimized DAG to fetch SESAR samples in parallel using traditional operators.',
    schedule_interval=None,
    start_date=datetime(2025, 7, 10),
    catchup=False,
    tags=['sesar', 'api', 'etl'],
) as dag:

    setup_tracking_table = PostgresOperator(
        task_id='setup_tracking_table',
        postgres_conn_id=DB_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS sesar_processed_hexes (
                hex_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """
    )

    get_hexes_task = PythonOperator(
        task_id='get_unprocessed_hexes',
        python_callable=_get_unprocessed_hexes
    )

    # This is a template task that will be expanded into many parallel tasks.
    fetch_igsns_task = PythonOperator(
        task_id='fetch_igsns_for_hex',
        python_callable=_fetch_igsns_for_hex,
    )
    
    load_metadata_task = PythonOperator(
        task_id='load_sesar_metadata',
        python_callable=_load_sesar_metadata,
    )

    # --- Task Orchestration with Dynamic Mapping ---
    setup_tracking_table >> get_hexes_task

    # Use the output of `get_hexes_task` to create a parallel task for each hex.
    # `op_args` expects a list of lists, where each inner list contains the arguments
    # for one mapped task instance.
    fetch_igsns_task.expand(
        op_args=get_hexes_task.output.map(lambda x: [x])
    )

    fetch_igsns_task >> load_metadata_task