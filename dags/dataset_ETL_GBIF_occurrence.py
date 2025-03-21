from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import pygbif
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

# Function to load H3 data to PostgreSQL
def function1(argument, argument, anotherargument):
    python code things

# Define the DAG
dag = DAG(
    'its_my_dag',
    default_args=default_args,
    description='DAG to do the DAG',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Define task arguments
URL_I_NEED = "http://url.i.need"
CSV_I_NEED = "csvineed.csv"

# task 1 python operator
task1 = PythonOperator(
    task_id='task1',
    python_callable=function1,
    op_kwargs={
        'csv_path': OUTPUT_CSV,
        'table_name': 'table',
        'postgres_conn_id': 'connection',
    },
    dag=dag,
)

# task 2 python operator
task2 = PythonOperator(
    task_id='task2',
    python_callable=function2,
    op_kwargs={
        'csv_path': OUTPUT_CSV,
        'table_name': 'table',
        'postgres_conn_id': 'connection',
    },
    dag=dag,
)

# Task3 postgresql operator
task3 = PostgresOperator(
    task_id='task3',
    postgres_conn_id='connection',  # Define your connection ID
    sql="""
        DO SOME SQL HERE
    """,
)


# Set task dependencies
task1 >> task2 >> task3 
