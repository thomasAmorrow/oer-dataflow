from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import Integer, Float, String, Boolean, Date
import logging

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Function to load CSV data using pandas & SQLAlchemy
def load_csv_to_postgres(csv_path, schema_path, dataframe_chunk_size=200000, sql_chunk_size=1000):
    logging.info("Starting load_csv_to_postgres function")
    
    logging.info("Reading schema file")
    schema_df = pd.read_csv(schema_path)
    expected_headers = schema_df.iloc[:, 0].tolist()
    expected_headers = [header.strip().lower() for header in expected_headers] # strip whitespace and lowercase headers
    data_types = schema_df.iloc[:, 4].tolist()
    
    logging.info("Establishing connection with Postgres")
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    engine = create_engine(postgres_hook.get_uri())
    metadata = MetaData()

    # Read CSV headers
    logging.info("Validating CSV headers")
    df_headers = pd.read_csv(csv_path, nrows=0)
    csv_headers = [col.strip().lower() for col in df_headers.columns.tolist()[3:]]  # Drop first 3 columns and normalize case
    
    # Map schema data types to SQLAlchemy types
    type_mapping = {
        'Logical': Integer,
        'Character': String,
        'Integer': Integer,
        'Factor': String,
        'Float': Float,
        'Date': String  # string works due to years (date in schema) being detected as integers
    }
    
    columns = [Column(col, type_mapping.get(dtype, String)) for col, dtype in zip(expected_headers, data_types)]
    
    logging.info("Defining SQLAlchemy table")
    table = Table('dscrtp', metadata, *columns)

    # Drop and recreate the table only once
    with engine.connect() as connection:
        logging.info("Dropping existing table if it exists")
        connection.execute("DROP TABLE IF EXISTS dscrtp")
        logging.info("Table 'dscrtp' dropped if it existed.")

        result = connection.execute("SELECT to_regclass('dscrtp')")
        table_exists = result.fetchone()[0]
        if table_exists is not None:
            raise Exception("Table 'dscrtp' was not dropped successfully. Load will not proceed.")
        else:
            logging.info("Table 'dscrtp' confirmed dropped.")

        logging.info("Creating new table")
        metadata.create_all(engine)
        logging.info("Table created successfully")

    # Load data in chunks
    with engine.connect() as connection:
        logging.info("Starting batch data load")
        for i, df_chunk in enumerate(pd.read_csv(csv_path, chunksize=dataframe_chunk_size, low_memory=False)):
            df_chunk = df_chunk.iloc[:, 3:]  # Drop first 3 columns
            df_chunk.columns = [col.strip().lower() for col in df_chunk.columns]  # Normalize column names
            logging.info(f"Processing DataFrame batch {i + 1}")
            
            # Convert data types
            for col, dtype in zip(expected_headers, data_types):
                if dtype in ['Integer', 'Float'] and col in df_chunk.columns:
                    df_chunk[col] = pd.to_numeric(df_chunk[col], errors='coerce')
            
            # Insert in smaller SQL batches
            for j in range(0, len(df_chunk), sql_chunk_size):
                chunk = df_chunk.iloc[j:j + sql_chunk_size]
                chunk.to_sql('dscrtp', connection, if_exists='append', index=False, method='multi', chunksize=sql_chunk_size)
                logging.info(f"Inserted SQL batch {j // sql_chunk_size + 1} from DataFrame batch {i + 1}")
    logging.info("Data loading complete")

# Define DAG
dag = DAG(
    dag_id='dataset_create_load_DSCRTP_batch',
    default_args=default_args,
    description='Create DSCRTP table and load CSV using SQLAlchemy with schema validation',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

# Define the task
load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'csv_path': '/tmp/DSCRTP_NatDB_cleaned.csv',
        'schema_path': '/tmp/DSCRTP_Schema.csv',
        'dataframe_chunk_size': 200000,
        'sql_chunk_size': 1000
    },
    dag=dag
)
