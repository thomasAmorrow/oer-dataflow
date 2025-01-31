from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ftplib import FTP
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def download_csv_ftp(ftp_host, ftp_directory, ftp_filename, output_path):
    """Download a CSV file from an FTP server and save it locally."""
    with FTP(ftp_host) as ftp:
        ftp.login()  # Anonymous login
        ftp.cwd(ftp_directory)  # Change to the directory
        with open(output_path, 'wb') as f:
            ftp.retrbinary(f'RETR {ftp_filename}', f.write)  # Retrieve the file
        print(f"CSV downloaded and saved to {output_path}")

# Define the DAG
with DAG(
    dag_id='dataset_get_ftp_DSCRTP_csv',
    default_args=default_args,
    description='A DAG to download a CSV from an FTP server',
    schedule_interval=timedelta(days=180),  # Run every 180 days
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['csv', 'download', 'ftp'],
) as dag:

    # Define task to download the first CSV via FTP
    download_csv_task_1 = PythonOperator(
        task_id='dataset_get_ftp_DSCRTP_csv_1',
        python_callable=download_csv_ftp,
        op_kwargs={
            'ftp_host': 'ftp-oceans.ncei.noaa.gov',
            'ftp_directory': 'nodc/archive/arc0087/0145037/6.6/data/0-data/',
            'ftp_filename': 'DSCRTP_NatDB_20241022-1.csv',
            'output_path': '/tmp/DSCRTP_NatDB.csv',
        },
    )

    # Define task to download the second CSV via FTP
    download_csv_task_2 = PythonOperator(
        task_id='dataset_get_ftp_DSCRTP_csv_2',
        python_callable=download_csv_ftp,
        op_kwargs={
            'ftp_host': 'ftp-oceans.ncei.noaa.gov',
            'ftp_directory': 'nodc/archive/arc0087/0145037/6.6/data/0-data/',
            'ftp_filename': '2022_DSCRTP_National_Database_Schema.csv',
            'output_path': '/tmp/DSCRTP_Schema.csv',
        },
    )

    # Task sequence: First download, then the second one
    download_csv_task_1 >> download_csv_task_2
