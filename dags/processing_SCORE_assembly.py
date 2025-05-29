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
create_primary_SCORE_table= PostgresOperator(
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
    WHERE ega_score_05.hex_05 = glodap.hex_05;

    ALTER TABLE ega_score_05
    ADD COLUMN geology_score FLOAT;

    UPDATE ega_score_05
    SET geology_score = 1
    FROM imlgs
    WHERE ega_score_05.hex_05 = imlgs.hex_05;

    ALTER TABLE ega_score_05
    ADD COLUMN edna_score FLOAT;

    UPDATE ega_score_05
    SET edna_score = 1
    FROM obis_sequences
    WHERE ega_score_05.hex_05 = obis_sequences.hex_05;

    ALTER TABLE ega_score_05
    ADD COLUMN wcsd_score FLOAT;

    UPDATE ega_score_05
    SET wcsd_score = 1
    FROM wcsd_footprints
	WHERE ega_score_05.hex_05 = wcsd_footprints.hex_05;

    """,
    dag=dag,
)

# Task: Create the h3_children table
combine_primary_scores= PostgresOperator(
    task_id='combine_scores',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
        ALTER TABLE ega_score_05
        ADD COLUMN combined_score FLOAT;

        UPDATE ega_score_05
        SET combined_score = (COALESCE(mapping_score, 0) + COALESCE(occurrence_score, 0) + COALESCE(chemistry_score, 0) + COALESCE(geology_score, 0) + COALESCE(edna_score, 0) + COALESCE(wcsd_score, 0)) / 6;

    """,
    dag=dag,
)

# Task: propagate scores upwards
derez_scores_to_parents= PostgresOperator(
    task_id='derez_scores_to_parents',
    postgres_conn_id='oceexp-db',
    sql="""
        DROP TABLE IF EXISTS ega_score_04;

        CREATE TABLE ega_score_04 AS
        WITH hex_04_stats AS (
            SELECT 
                h3_cell_to_parent(hex_05, 4) AS hex_04,
                SUM(mapping_score) AS mapping_score_sum, 
                SUM(occurrence_score) AS occurrence_score_sum,
                SUM(chemistry_score) AS chemistry_score_sum,
                SUM(geology_score) AS geology_score_sum,
                SUM(edna_score) AS edna_score_sum,
                SUM(wcsd_score) AS wcsd_score_sum
            FROM ega_score_05
            GROUP BY h3_cell_to_parent(hex_05, 4)
        ),
        child_counts AS (
            SELECT 
                p.hex_04,
                COUNT(child.hex) AS num_children
            FROM (
                SELECT DISTINCT h3_cell_to_parent(hex_05, 4) AS hex_04
                FROM ega_score_05
            ) AS p,
            LATERAL (
                SELECT h3_cell_to_children(p.hex_04, 5) AS hex
            ) AS child
            GROUP BY p.hex_04
        )
        SELECT 
            h.hex_04,
            mapping_score_sum / num_children AS mapping_score,
            occurrence_score_sum / num_children AS occurrence_score,
            chemistry_score_sum / num_children AS chemistry_score,
            geology_score_sum / num_children AS geology_score,
            edna_score_sum / num_children AS edna_score,
            wcsd_score_sum / num_children AS wcsd_Score
        FROM hex_04_stats h
        JOIN child_counts c ON h.hex_04 = c.hex_04;

        ALTER TABLE ega_score_04
        ADD COLUMN combined_score FLOAT;

        UPDATE ega_score_04
        SET combined_score = (COALESCE(mapping_score, 0) + COALESCE(occurrence_score, 0) + COALESCE(chemistry_score, 0) + COALESCE(geology_score, 0) + COALESCE(edna_score, 0) + COALESCE(wcsd_score, 0)) / 6;

        ALTER TABLE ega_score_04
        ADD PRIMARY KEY (hex_04);

        DROP TABLE IF EXISTS ega_score_03;

        CREATE TABLE ega_score_03 AS
        WITH hex_03_stats AS (
            SELECT 
                h3_cell_to_parent(hex_05, 3) AS hex_03,
                SUM(mapping_score) AS mapping_score_sum, 
                SUM(occurrence_score) AS occurrence_score_sum,
                SUM(chemistry_score) AS chemistry_score_sum,
                SUM(geology_score) AS geology_score_sum,
                SUM(edna_score) AS edna_score_sum,
                SUM(wcsd_score) AS wcsd_score_sum
            FROM ega_score_05
            GROUP BY h3_cell_to_parent(hex_05, 3)
        ),
        child_counts AS (
            SELECT 
                p.hex_03,
                COUNT(child.hex) AS num_children
            FROM (
                SELECT DISTINCT h3_cell_to_parent(hex_05, 3) AS hex_03
                FROM ega_score_05
            ) AS p,
            LATERAL (
                SELECT h3_cell_to_children(p.hex_03, 5) AS hex
            ) AS child
            GROUP BY p.hex_03
        )
        SELECT 
            h.hex_03,
            mapping_score_sum / num_children AS mapping_score,
            occurrence_score_sum / num_children AS occurrence_score,
            chemistry_score_sum / num_children AS chemistry_score,
            geology_score_sum / num_children AS geology_score,
            edna_score_sum / num_children AS edna_score,
            wcsd_score_sum / num_children AS wcsd_score
        FROM hex_03_stats h
        JOIN child_counts c ON h.hex_03 = c.hex_03;

        ALTER TABLE ega_score_03
        ADD COLUMN combined_score FLOAT;

        UPDATE ega_score_03
        SET combined_score = (COALESCE(mapping_score, 0) + COALESCE(occurrence_score, 0) + COALESCE(chemistry_score, 0) + COALESCE(geology_score, 0) + COALESCE(edna_score, 0) + COALESCE(wcsd_score, 0)) / 6;

        ALTER TABLE ega_score_03
        ADD PRIMARY KEY (hex_03);
    """,
    dag=dag
)

# Define task dependencies
create_primary_SCORE_table >> combine_primary_scores >> derez_scores_to_parents