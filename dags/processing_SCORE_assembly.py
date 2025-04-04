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
    description='DAG to download, process, and save OSM water hexagons',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Task: Create the h3_children table
create_SCORE_table= PostgresOperator(
    task_id='create_SCORE_table',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
    DROP TABLE IF EXISTS ega_score_05

    CREATE TABLE ega_score_05 AS
    SELECT hex_05
    FROM h3_oceans;

    ALTER TABLE ega_score_05
    ADD COLUMN mapping_score FLOAT;

    UPDATE ega_score_05
    SET mapping_score = CASE
                        WHEN m.val BETWEEN 10 AND 17 THEN 1
                        WHEN m.val > 17 THEN 0.1
                        ELSE 0
                    END
    FROM ega_score_05 s
    LEFT JOIN gebco_tid_hex m
    ON s.hex_05 = m.hex_05;


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

    )
    """,
)

# Task: Create the h3_children table
assemble_scores= PostgresOperator(
    task_id='assemble_scores',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
        UPDATE ega_score_05
        SET chemistry_score = 1
        FROM glodap
        WHERE ega_score_05.hex_05 = glodap.hex_05;
    """,
)


# Define task dependencies
create_SCORE_table