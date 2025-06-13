from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import time
import logging

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
    'processing_tie_data_sources',
    default_args=default_args,
    description='DAG to tie data sources to hexagons',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Python task: Fetch and store GBIF dataset metadata
def fetch_and_store_gbif_datasets():
    hook = PostgresHook(postgres_conn_id='oceexp-db')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gbif_references (
            datasetkey TEXT PRIMARY KEY,
            dataset JSONB
        );
    """)

    # Clear old data
    cursor.execute("TRUNCATE TABLE gbif_references;")

    # Get unique datasetkeys
    cursor.execute("SELECT DISTINCT datasetkey FROM gbif_occurrences;")
    keys = cursor.fetchall()

    for (key,) in keys:
        url = f"https://api.gbif.org/v1/dataset/{key}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                dataset_json = response.json()
                cursor.execute(
                    """
                    INSERT INTO gbif_references (datasetkey, dataset)
                    VALUES (%s, %s)
                    ON CONFLICT (datasetkey) DO UPDATE SET dataset = EXCLUDED.dataset;
                    """,
                    (key, json.dumps(dataset_json))
                )
            else:
                logging.warning(f"Failed to fetch {key}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching {key}: {e}")
        time.sleep(0.2)  # Rate limiting

    conn.commit()
    cursor.close()
    conn.close()

# Task: Use PythonOperator instead of PostgresOperator
tie_gbif_sources = PythonOperator(
    task_id='tie_gbif_sources',
    python_callable=fetch_and_store_gbif_datasets,
    dag=dag,
)

# Set task dependency
tie_gbif_sources
