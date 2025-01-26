from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dscrtp_data (
        "Entry" INTEGER,
        "Flag" INTEGER,
        "FlagReason" FLOAT,
        "ShallowFlag" INTEGER,
        "DatabaseVersion" TEXT,
        "DatasetID" TEXT,
        "CatalogNumber" INTEGER,
        "SampleID" TEXT,
        "TrackingID" TEXT,
        "ImageURL" FLOAT,
        "HighlightImageURL" FLOAT,
        "Citation" TEXT,
        "Repository" TEXT,
        "ScientificName" TEXT,
        "VerbatimScientificName" TEXT,
        "VernacularNameCategory" TEXT,
        "VernacularName" FLOAT,
        "TaxonRank" TEXT,
        "AphiaID" INTEGER,
        "LifeScienceIdentifier" TEXT,
        "Phylum" TEXT,
        "Class" TEXT,
        "Subclass" TEXT,
        "Order" TEXT,
        "Suborder" TEXT,
        "Family" TEXT,
        "Subfamily" TEXT,
        "Genus" TEXT,
        "Subgenus" TEXT,
        "Species" TEXT,
        "Subspecies" TEXT,
        "ScientificNameAuthorship" TEXT,
        "TypeStatus" TEXT,
        "OperationalTaxonomicUnit" TEXT,
        "Morphospecies" TEXT,
        "CombinedNameID" TEXT,
        "Synonyms" TEXT,
        "IdentificationComments" TEXT,
        "IdentifiedBy" TEXT,
        "IdentificationDate" FLOAT,
        "IdentificationQualifier" TEXT,
        "IdentificationVerificationStatus" INTEGER,
        "AssociatedSequences" FLOAT,
        "Ocean" TEXT,
        "LargeMarineEcosystem" TEXT,
        "Country" TEXT,
        "FishCouncilRegion" TEXT,
        "Locality" TEXT,
        "Latitude" FLOAT,
        "Longitude" FLOAT,
        "DepthInMeters" INTEGER,
        "DepthMethod" TEXT,
        "MinimumDepthInMeters" INTEGER,
        "MaximumDepthInMeters" INTEGER,
        "LocationComments" TEXT,
        "ObservationDate" TEXT,
        "ObservationYear" INTEGER,
        "ObservationTime" FLOAT,
        "SurveyID" TEXT,
        "Vessel" TEXT,
        "PI" TEXT,
        "PIAffiliation" TEXT,
        "Purpose" TEXT,
        "SurveyComments" TEXT,
        "Station" INTEGER,
        "EventID" FLOAT,
        "TA" FLOAT,
        "DIC" FLOAT,
        "RecordType" TEXT,
        "ImageFilePath" TEXT,
        "HighlightImageFilePath" TEXT,
        "DataProvider" TEXT,
        "DataContact" TEXT,
        "Modified" TEXT,
        "EntryUpdate" TEXT,
        "WebSite" TEXT,
        "EntryDate" TEXT,
        "Reporter" TEXT,
        "ReporterEmail" TEXT,
        "ReporterComments" TEXT,
        "AccessionID" TEXT,
        "gisLandCheck" INTEGER,
        "gisCRMDepth" INTEGER,
        "gisGEBCODepth" INTEGER,
        "gisEtopoDepth" INTEGER,
        "gisNGIALocality" TEXT,
        "gisGEBCOLocality" TEXT,
        "gisNGIADist" FLOAT,
        "gisGEBCODist" FLOAT,
        "gisLME" TEXT,
        "gisMEOW" TEXT,
        "gisIHOSeas" TEXT,
        "gisCaribbeanFMC" INTEGER,
        "gisGulfOfMexicoFMC" INTEGER,
        "gisMidAtlanticFMC" INTEGER,
        "gisNewEnglandFMC" INTEGER,
        "gisNorthPacificFMC" INTEGER,
        "gisPacificFMC" INTEGER,
        "gisSouthAtlanticFMC" INTEGER,
        "gisWesternPacificFMC" INTEGER
    );
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.run(create_table_query)

def load_csv_to_postgres():
    # Specify the CSV file path (it must be accessible to the PostgreSQL server)
    csv_path = '/tmp/DSCRTP_NatDB.csv'

    # COPY command to load CSV data into PostgreSQL
    copy_query = f"""
    COPY dscrtp_data FROM '{csv_path}' WITH CSV HEADER DELIMITER ',' NULL 'NA';
    """
    
    # PostgreSQL hook to run the COPY query
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.run(copy_query)

with DAG(
    dag_id='dataset_load_DSCRTP_to_postgres',
    default_args=default_args,
    description='Load DSCRTP CSV into PostgreSQL with schema creation',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    create_table_task >> load_csv_task
