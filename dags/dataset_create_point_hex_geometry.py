from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the function to execute SQL queries
def create_point_hex_geometry():
    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

        ALTER TABLE dscrtp ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);
        
        UPDATE dscrtp 
        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) 
        WHERE geom IS NULL;
    """

    # Generate SQL for H3 columns and updates for resolutions 1 to 15
    for res in range(1, 16):  # Resolutions 1 to 15
        sql_statements += f"""
            ALTER TABLE dscrtp ADD COLUMN IF NOT EXISTS h3hex{res} VARCHAR(15);

            UPDATE dscrtp 
            SET h3hex{res} = h3_lat_lng_to_cell(geom, {res}) 
            WHERE h3hex{res} IS NULL;
        """

    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    
    try:
        logging.info("Executing SQL statements...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

# Define the DAG
dag = DAG(
    dag_id='dataset_create_point_hex_geometry',
    default_args=default_args,
    description='DAG to create point geometry and generate H3 hexagons for resolutions 1 to 15',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=days_ago(1),
    catchup=False,
)

# Define the task
run_sql_task = PythonOperator(
    task_id='create_point_hex_geometry',
    python_callable=create_point_hex_geometry,
    dag=dag,
)

# Task dependency (if needed, more tasks can be added)
run_sql_task
