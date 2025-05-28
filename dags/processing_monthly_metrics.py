from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=20),
}

# Define the DAG
dag = DAG(
    'processing_monthly_metrics',
    default_args=default_args,
    description='DAG to conduct monthly metrics assessment',
    schedule_interval='@monthly',
    start_date=datetime(2025, 5, 1),
    catchup=False,
)

backup_db = TriggerDagRunOperator(
    task_id='backup_db',
    trigger_dag_id='output_pgdump',  # your actual DAG ID
    wait_for_completion=True,
    reset_dag_run=True,
)

trigger_ETL = TriggerDagRunOperator(
    task_id='trigger_ETL',
    trigger_dag_id='trigger_dataset_ETL_series',  # your actual DAG ID
    wait_for_completion=True,
    reset_dag_run=True,
)

trigger_score_assembly = TriggerDagRunOperator(
    task_id='trigger_score_assembly',
    trigger_dag_id='processing_SCORE_assembly',  # your actual DAG ID
    wait_for_completion=True,
    reset_dag_run=True,
)

trigger_cleanup = TriggerDagRunOperator(
    task_id='trigger_cleanup',
    trigger_dag_id='processing_cleanup_and_fk',  # your actual DAG ID
    wait_for_completion=True,
    reset_dag_run=True,
)

count_store_explored = PostgresOperator(
    task_id='count_store_explored',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
    CREATE TABLE IF NOT EXISTS metrics_monthly (
        date DATE,
        hexs_combined_explored INT,
        hexperc_combined_explored FLOAT,
        obs_combined INT,
        hexs_mapped_explored INT,
        hexperc_mapped_explored FLOAT,
        obs_mapped INT,
        hexs_occurrence_explored INT,
        hexperc_occurrence_explored FLOAT,
        obs_occurrence INT,
        hexs_chemistry_explored INT,
        hexperc_chemistry_explored FLOAT,
        obs_chemistry INT,
        hexs_geology_explored INT,
        hexperc_geology_explored FLOAT,
        obs_geology INT,
        hexs_edna_explored INT,
        hexperc_edna_explored FLOAT,
        obs_edna INT,
        hexs_wcsd_explored INT,
        hexperc_wcsd_explored FLOAT,
        obs_wcsd INT,
        hexs_mapexempt_explored INT,
        hexperc_mapexempt_explored FLOAT,
        obs_mapexempt_explored INT
    );

    INSERT INTO metrics_monthly (
        date,
        hexs_combined_explored,
        hexperc_combined_explored,
        obs_combined,
        hexs_mapped_explored,
        hexperc_mapped_explored,
        obs_mapped,
        hexs_occurrence_explored,
        hexperc_occurrence_explored,
        obs_occurrence,
        hexs_chemistry_explored,
        hexperc_chemistry_explored,
        obs_chemistry,
        hexs_geology_explored,
        hexperc_geology_explored,
        obs_geology,
        hexs_edna_explored,
        hexperc_edna_explored,
        obs_edna,
        hexs_wcsd_explored,
        hexperc_wcsd_explored,
        obs_wcsd,
        hexs_mapexempt_explored,
        hexperc_mapexempt_explored,
        obs_mapexempt_explored
        )
    VALUES (
        CURRENT_DATE,
        (SELECT COUNT(*) FROM ega_score_05 WHERE combined_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE combined_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM glodap) + (SELECT COUNT(*) FROM imlgs) + (SELECT COUNT(*) FROM obis_sequences) + (SELECT COUNT(*) FROM gbif_occurrences) + (SELECT COUNT(*) FROM gebco_tid_hex) + (SELECT COUNT(*) FROM wcsd_lines),
        (SELECT COUNT(*) FROM ega_score_05 WHERE mapping_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE mapping_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM gebco_tid_hex),
        (SELECT COUNT(*) FROM ega_score_05 WHERE occurrence_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE occurrence_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM gbif_occurrences),
        (SELECT COUNT(*) FROM ega_score_05 WHERE chemistry_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE chemistry_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM glodap),
        (SELECT COUNT(*) FROM ega_score_05 WHERE geology_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE geology_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM imlgs),
        (SELECT COUNT(*) FROM ega_score_05 WHERE edna_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE edna_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM obis_sequences),
        (SELECT COUNT(*) FROM ega_score_05 WHERE wcsd_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE wcsd_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM wcsd_lines),
        (SELECT COUNT(*) FROM ega_score_05 WHERE occurrence_score > 0 OR chemistry_score > 0 OR geology_score > 0 OR edna_score > 0),
        (SELECT COUNT(*) FROM ega_score_05 WHERE occurrence_score > 0 OR chemistry_score > 0 OR geology_score > 0 OR edna_score > 0) * 1.0 / NULLIF((SELECT COUNT(*) FROM ega_score_05), 0),
        (SELECT COUNT(*) FROM glodap) + (SELECT COUNT(*) FROM imlgs) + (SELECT COUNT(*) FROM obis_sequences) + (SELECT COUNT(*) FROM gbif_occurrences)
    );
    """,
    dag=dag
)

backup_db >> trigger_ETL >> trigger_score_assembly >> trigger_cleanup >> count_store_explored