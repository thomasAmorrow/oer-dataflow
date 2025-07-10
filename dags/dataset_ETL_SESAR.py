import time
import logging
import json
import requests
import h3
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# Delay between API hits (in seconds)
API_HIT_DELAY = 1  # Adjustable via env var or Variable if needed
PAGE_LIMIT = 500  # Max samples per request

def crosses_antimeridian(boundary):
    longitudes = [lng for lat, lng in boundary]
    return any(
        lng <= -90 and longitudes[i+1] > 90 or
        lng >= 90 and longitudes[i+1] < -90
        for i, lng in enumerate(longitudes[:-1])
    )

def adjust_longitudes_if_crosses_antimeridian(boundary):
    return [
        [lat, lng + 360 if lng < 0 else lng]
        for lat, lng in boundary
    ]

def close_polygon(polygon):
    if not polygon:
        raise ValueError("Polygon is empty and cannot be closed.")
    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])
    return polygon

def fetch_hexes():
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT hex_03 FROM ega_score_03")
    hexes = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return hexes

def fetch_igsns_for_hexes():
    hexes = fetch_hexes()
    all_igsns = set()

    for h3_index in hexes:
        boundary_tuple = h3.cell_to_boundary(h3_index)
        boundary = list(boundary_tuple)

        if crosses_antimeridian(boundary):
            boundary = adjust_longitudes_if_crosses_antimeridian(boundary)

        boundary = close_polygon(boundary)
        geostring = ",".join([f"{round(lng, 6)} {round(lat, 6)}" for lat, lng in boundary])

        page = 1
        while True:
            url = f"https://app.geosamples.org/samples/polygon/{geostring}?limit={PAGE_LIMIT}&page_no={page}&hide_private=1"
            headers = {'Accept': 'application/json'}
            try:
                logging.info(f"Searching IGSNs for hex {h3_index}...")
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                igsns = data.get("igsn_list", [])
                if not igsns:
                    logging.info(f"No IGSNs found for hex {h3_index} (page {page}) — skipping.")
                    break
                all_igsns.update(igsns)
                if len(igsns) < PAGE_LIMIT:
                    break
                page += 1
                time.sleep(API_HIT_DELAY)
            except Exception as e:
                logging.warning(f"Failed fetching IGSNs for {h3_index}: {e}")
                break

    with open("/mnt/bucket/sesar_igsns.json", "w") as f:
        json.dump(list(all_igsns), f)

def fetch_and_store_metadata():
    with open("/mnt/bucket/sesar_igsns.json") as f:
        igsn_list = json.load(f)

    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sesar_samples (
            igsn TEXT PRIMARY KEY,
            material TEXT,
            classification TEXT,
            field_name TEXT,
            description TEXT,
            collection_method TEXT,
            collection_method_desc TEXT,
            size TEXT,
            geological_age TEXT,
            geological_unit TEXT,
            comment TEXT,
            purpose TEXT,
            lat_start FLOAT,
            lat_end FLOAT,
            lon_start FLOAT,
            lon_end FLOAT,
            elevation_start FLOAT,
            elevation_end FLOAT,
            physiographic_feature TEXT,
            physiographic_feature_name TEXT,
            location_desc TEXT,
            locality TEXT,
            locality_desc TEXT,
            country TEXT,
            state_province TEXT,
            county TEXT,
            city TEXT,
            field_program TEXT,
            platform_type TEXT,
            platform_name TEXT,
            platform_desc TEXT,
            launch_type TEXT,
            launch_platform_name TEXT,
            launch_id TEXT,
            collector TEXT,
            collector_detail TEXT,
            collection_date_start DATE,
            collection_date_end DATE,
            archive TEXT,
            archive_contact TEXT,
            orig_archive TEXT,
            orig_archive_contact TEXT,
            parent_igsns TEXT[],
            sibling_igsns TEXT[],
            child_igsns TEXT[],
            raw_json JSONB
        );
    """)

    for igsn in igsn_list:
        url = f"https://app.geosamples.org/sample/igsn/{igsn}"
        headers = {'Accept': 'application/json'}
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            sample = data.get("sample", {})

            cursor.execute(
                """
                INSERT INTO sesar_samples (igsn, material, classification, field_name, description,
                    collection_method, collection_method_desc, size, geological_age, geological_unit,
                    comment, purpose, lat_start, lat_end, lon_start, lon_end, elevation_start, elevation_end,
                    physiographic_feature, physiographic_feature_name, location_desc, locality, locality_desc,
                    country, state_province, county, city, field_program, platform_type, platform_name,
                    platform_desc, launch_type, launch_platform_name, launch_id, collector, collector_detail,
                    collection_date_start, collection_date_end, archive, archive_contact, orig_archive,
                    orig_archive_contact, parent_igsns, sibling_igsns, child_igsns, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (igsn) DO NOTHING;
                """,
                (
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
            )
            conn.commit()
            time.sleep(API_HIT_DELAY)
        except Exception as e:
            logging.warning(f"Failed fetching metadata for IGSN {igsn}: {e}")

    cursor.close()
    conn.close()

# DAG setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dataset_ETL_SESAR',
    default_args=default_args,
    description='Fetch SESAR samples from GeoSamples API using H3 polygons and save to PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2025, 7, 10),
    catchup=False,
)

fetch_sesar_igsns = PythonOperator(
    task_id='fetch_sesar_igsns',
    python_callable=fetch_igsns_for_hexes,
    provide_context=True,
    dag=dag
)

load_sesar_metadata = PythonOperator(
    task_id='load_sesar_metadata',
    python_callable=fetch_and_store_metadata,
    provide_context=True,
    dag=dag
)

fetch_sesar_igsns >> load_sesar_metadata
