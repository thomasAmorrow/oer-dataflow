from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import time

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Function to load CSV data using pandas & SQLAlchemy
def load_csv_to_postgres():
    csv_path = '/tmp/DSCRTP_NatDB_cleaned.csv'
    batch_size = 1000  # Adjust for performance
    
    # Establish connection with Postgres using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    engine = create_engine(postgres_hook.get_uri())
    metadata = MetaData()

    # Read the CSV file to DataFrame
    df = pd.read_csv(csv_path, dtype=str)

    # Drop the table if it exists (recreate it)
    with engine.connect() as connection:
        connection.execute("DROP TABLE IF EXISTS dscrtp")
    
    # Load DataFrame to PostgreSQL table (create a new one)
    with engine.connect() as connection:
        for chunk in pd.read_csv(csv_path, chunksize=batch_size, dtype=str):
            start_time = time.time()  # Record start time for batch
            
            # Insert the chunk into the database
            chunk.to_sql('dscrtp', connection, if_exists='append', index=False, method='multi', chunksize=batch_size)
            
            end_time = time.time()  # Record end time for batch
            batch_time = end_time - start_time  # Calculate time taken for the batch
            
            # Log the time taken for this batch
            print(f"Batch processed in {batch_time:.2f} seconds")

# Define DAG
dag = DAG(
    dag_id='dataset_create_load_DSCRTP_batch',
    default_args=default_args,
    description='Create DSCRTP table and load CSV using SQLAlchemy',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

# Define the task
load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)
