from airflow import DAG
from airflow.providers.ftp.operators.ftp_to_file import FTPToFileOperator
from datetime import datetime, timedelta

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ftp_to_local_dag',
    default_args=default_args,
    description='Download files from FTP',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Download file from FTP
    download_file = FTPToFileOperator(
        task_id='download_file',
        ftp_conn_id='EXData_GFOE',
        remote_path='/OkeanosCruises/EX2301/CTD/SHIPCTD/EX2301_CTD001_20230415T133000Z.cnv',
        local_path='/tmp/EX2301_CTD001_20230415T133000Z.cnv',
    )

