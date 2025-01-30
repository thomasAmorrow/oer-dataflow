from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import NoSuchTableError

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
    batch_size = 20  # Adjust for performance
    
    # Establish connection with Postgres using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    engine = create_engine(postgres_hook.get_uri())
    metadata = MetaData()

    # Read the CSV file to DataFrame (no chunks for now, just load once)
    df = pd.read_csv(csv_path, dtype=str)

    # Try to reflect the existing table
    try:
        table = Table('dscrtp', metadata, autoload_with=engine)
    except NoSuchTableError:
        # If the table doesn't exist, create it based on the DataFrame schema
        df.head(0).to_sql('dscrtp', engine, if_exists='replace', index=False, method='multi')

    # Load DataFrame to PostgreSQL table in batches
    df.to_sql('dscrtp', engine, if_exists='append', index=False, method='multi', chunksize=batch_size)

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
