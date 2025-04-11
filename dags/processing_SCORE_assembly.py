from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator


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
    'processing_SCORE_assembly',
    default_args=default_args,
    description='DAG to assemble exploration gap scores from observations',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Task: Create the scores table
create_SCORE_table= PostgresOperator(
    task_id='create_SCORE_table',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
    DROP TABLE IF EXISTS ega_score_05;

    CREATE TABLE IF NOT EXISTS ega_score_05 AS
    SELECT hex_05
    FROM h3_oceans;

    ALTER TABLE ega_score_05
    ADD PRIMARY KEY (hex_05);

    ALTER TABLE ega_score_05
    ADD COLUMN mapping_score FLOAT;

    UPDATE ega_score_05
    SET mapping_score = 1
    FROM gebco_tid_hex
    WHERE ega_score_05.hex_05 = gebco_tid_hex.hex_05 AND gebco_tid_hex.val BETWEEN 9 AND 18;

    UPDATE ega_score_05
    SET mapping_score = 0.1
    FROM gebco_tid_hex
    WHERE ega_score_05.hex_05 = gebco_tid_hex.hex_05 AND gebco_tid_hex.val BETWEEN 39 AND 47;

    ALTER TABLE ega_score_05
    ADD COLUMN occurrence_score FLOAT;

    UPDATE ega_score_05
    SET occurrence_score = 1
    FROM gbif_occurrences
    WHERE ega_score_05.hex_05 = gbif_occurrences.hex_05;

    ALTER TABLE ega_score_05
    ADD COLUMN chemistry_score FLOAT;

    UPDATE ega_score_05
    SET chemistry_score = 1
    FROM glodap
    WHERE ega_score_05.hex_05 = glodap.hex_05

    """,
    dag=dag,
)

# Task: Create the h3_children table
combine_scores= PostgresOperator(
    task_id='combine_scores',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
        ALTER TABLE ega_score_05
        ADD COLUMN combined_score FLOAT;

        UPDATE ega_score_05
        SET combined_score = (COALESCE(mapping_score, 0) + COALESCE(occurrence_score, 0) + COALESCE(chemistry_score, 0)) / 3;

        ALTER TABLE ega_score_05
        ADD COLUMN h3_boundary_geom geometry(Polygon, 4326);

        UPDATE ega_score_05
        SET h3_boundary_geom = ST_SetSRID(h3_cell_to_boundary(hex_05), 4326);

        CREATE INDEX geom_idx
        ON ega_score_05
        USING GIST(h3_boundary_geom);

    """,
    dag=dag,
)


# Define task dependencies
create_SCORE_table >> combine_scores