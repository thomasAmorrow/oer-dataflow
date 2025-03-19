from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 17),
    'retries': 0,
}

# Initialize the DAG
with DAG(
    'dataset_create_child_hexagons',
    default_args=default_args,
    schedule_interval=None,  # You can set your schedule interval here
    catchup=False,
) as dag:

    # Task: Create the h3_children table
    create_h3_children = PostgresOperator(
        task_id='create_h3_children',
        postgres_conn_id='your_postgres_connection_id',  # Define your connection ID
        sql="""
            CREATE TABLE h3_children_08 AS
            SELECT
                child_hexagon
            FROM
                hex_ocean_polys_06,
                LATERAL H3_Cell_to_Children(CAST("h3_index" AS H3Index), 8) AS child_hexagon;
        """,
    )

    create_h3_children
