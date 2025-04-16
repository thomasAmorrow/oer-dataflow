from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

# Function to create and combine scores for each hex value
def process_scores_for_hex(**kwargs):
    hex_values = ['hex_05', 'hex_04', 'hex_03']
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    
    for hex_value in hex_values:
        # Create the scores table for the given hex value
        create_sql = f"""
        DROP TABLE IF EXISTS ega_score_{hex_value};

        CREATE TABLE IF NOT EXISTS ega_score_{hex_value} AS
        SELECT {hex_value}
        FROM h3_oceans;

        ALTER TABLE ega_score_{hex_value}
        ADD PRIMARY KEY ({hex_value});

        ALTER TABLE ega_score_{hex_value}
        ADD COLUMN mapping_score FLOAT;

        UPDATE ega_score_{hex_value}
        SET mapping_score = 1
        FROM gebco_tid_hex
        WHERE ega_score_{hex_value}.{hex_value} = gebco_tid_hex.{hex_value} AND gebco_tid_hex.val BETWEEN 9 AND 18;

        UPDATE ega_score_{hex_value}
        SET mapping_score = 0.1
        FROM gebco_tid_hex
        WHERE ega_score_{hex_value}.{hex_value} = gebco_tid_hex.{hex_value} AND gebco_tid_hex.val BETWEEN 39 AND 47;

        ALTER TABLE ega_score_{hex_value}
        ADD COLUMN occurrence_score FLOAT;

        UPDATE ega_score_{hex_value}
        SET occurrence_score = 1
        FROM gbif_occurrences
        WHERE ega_score_{hex_value}.{hex_value} = gbif_occurrences.{hex_value};

        ALTER TABLE ega_score_{hex_value}
        ADD COLUMN chemistry_score FLOAT;

        UPDATE ega_score_{hex_value}
        SET chemistry_score = 1
        FROM glodap
        WHERE ega_score_{hex_value}.{hex_value} = glodap.{hex_value};

        ALTER TABLE ega_score_{hex_value}
        ADD COLUMN geology_score FLOAT;

        UPDATE ega_score_{hex_value}
        SET geology_score = 1
        FROM imlgs
        WHERE ega_score_{hex_value}.{hex_value} = imlgs.{hex_value};
        """
        
        # Execute the creation SQL for the current hex value
        postgres_hook.run(create_sql)

        # Combine the scores for the given hex value
        combine_sql = f"""
            ALTER TABLE ega_score_{hex_value}
            ADD COLUMN combined_score FLOAT;

            UPDATE ega_score_{hex_value}
            SET combined_score = (COALESCE(mapping_score, 0) + COALESCE(occurrence_score, 0) + COALESCE(chemistry_score, 0) + COALESCE(geology_score, 0)) / 4;
        """
        
        # Execute the combine SQL for the current hex value
        postgres_hook.run(combine_sql)

# Define task to process all hex values
process_scores = PythonOperator(
    task_id='process_scores',
    python_callable=process_scores_for_hex,
    dag=dag,
)