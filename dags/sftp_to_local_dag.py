from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from datetime import datetime, timedelta

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sftp_to_local_dag',
    default_args=default_args,
    description='Download files from SFTP',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sftp', 'download'],
) as dag:

    get_file = SFTPOperator(
    	task_id='download_file',
        ssh_conn_id='GFOE_SSH',
        remote_filepath='OkeanosCruises/EX2301/CTD/SHIPCTD/EX2301_CTD001_20230415T133000Z.cnv',
        local_filepath='/tmp/EX2301_CTD001_20230415T133000Z.cnv',
        operation='get',
        create_intermediate_dirs=True,
    )

