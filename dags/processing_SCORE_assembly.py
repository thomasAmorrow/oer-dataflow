from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    DROP TABLE IF EXISTS ega_score

    CREATE TABLE ega_score_05(
    hex_05 H3INDEX PRIMARY KEY,
    mapping_score FLOAT,
    occurrence_score FLOAT,
    chemistry_score FLOAT
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
create_SCORE_table >> assemble_scores