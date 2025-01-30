from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import Integer, Float, String
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
    print("Starting load_csv_to_postgres function")
    csv_path = '/tmp/DSCRTP_NatDB_cleaned.csv'
    batch_size = 500  # Adjust for performance
    
    print("Establishing connection with Postgres")
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    engine = create_engine(postgres_hook.get_uri())
    metadata = MetaData()

    print("Reading CSV file")
    df = pd.read_csv(csv_path)
    print(f"CSV loaded with {len(df)} rows")

    # Map DataFrame columns to PostgreSQL column types
    columns = []
    for col in df.columns:
        if df[col].dtype == 'object':  # Check if the column is of type 'object' (strings)
            df[col] = df[col].str.strip()
        if pd.api.types.is_numeric_dtype(df[col]):
            if df[col].dtype == 'float64':
                columns.append(Column(col, Float))
            elif df[col].dtype == 'int64':
                columns.append(Column(col, Integer))
        else:
            columns.append(Column(col, String))

    print("Defining SQLAlchemy table")
    table = Table('dscrtp', metadata, *columns)

    # Drop the table if it exists
    with engine.connect() as connection:
        print("Dropping existing table if it exists")
        connection.execute("DROP TABLE IF EXISTS dscrtp")
        print("Table 'dscrtp' dropped if it existed.")

        result = connection.execute("SELECT to_regclass('dscrtp')")
        table_exists = result.fetchone()[0]
        if table_exists is not None:
            raise Exception(f"Table 'dscrtp' was not dropped successfully. Load will not proceed.")
        else:
            print("Table 'dscrtp' confirmed dropped.")

    # Load DataFrame to PostgreSQL table (create a new one)
    with engine.connect() as connection:
        print("Creating new table")
        metadata.create_all(engine)
        print("Table created successfully")

        # Start the batch loading process
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=batch_size, low_memory=False)):
            print(f"Processing batch {i + 1}")
            for col in chunk.select_dtypes(include=['float64', 'int64']).columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
            chunk.to_sql('dscrtp', connection, if_exists='append', index=False, method='multi', chunksize=batch_size)
            print(f"Batch {i + 1} inserted")
    print("Data loading complete")

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
