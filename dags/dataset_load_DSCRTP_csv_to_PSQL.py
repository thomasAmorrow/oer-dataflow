from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import duckdb

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
dag = DAG(
    'load_DSCRTP_to_postgres',
    default_args=default_args,
    description='Load DSCRTP CSV into PostgreSQL with schema creation',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

# these need to be checked against the DSCRTP csv
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dscrtp_data (
        "Unnamed: 0" INTEGER,
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
        "Subclass" FLOAT,
        "Order" TEXT,
        "Suborder" TEXT,
        "Family" TEXT,
        "Subfamily" FLOAT,
        "Genus" TEXT,
        "Subgenus" FLOAT,
        "Species" TEXT,
        "Subspecies" FLOAT,
        "ScientificNameAuthorship" TEXT,
        "TypeStatus" TEXT,
        "OperationalTaxonomicUnit" INTEGER,
        "Morphospecies" FLOAT,
        "CombinedNameID" FLOAT,
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
        "PI" FLOAT,
        "PIAffiliation" TEXT,
        "Purpose" FLOAT,
        "SurveyComments" FLOAT,
        "Station" INTEGER,
        "EventID" FLOAT,
        "SamplingEquipment" FLOAT,
        "VehicleName" FLOAT,
        "SampleAreaInSquareMeters" INTEGER,
        "footprintWKT" FLOAT,
        "footprintSRS" FLOAT,
        "IndividualCount" INTEGER,
        "CategoricalAbundance" TEXT,
        "Density" INTEGER,
        "Cover" FLOAT,
        "VerbatimSize" FLOAT,
        "MinimumSize" INTEGER,
        "MaximumSize" INTEGER,
        "WeightInKg" INTEGER,
        "Condition" TEXT,
        "AssociatedTaxa" FLOAT,
        "OccurrenceComments" FLOAT,
        "StartLatitude" INTEGER,
        "StartLongitude" INTEGER,
        "EndLatitude" INTEGER,
        "EndLongitude" INTEGER,
        "VerbatimLatitude" FLOAT,
        "VerbatimLongitude" FLOAT,
        "LocationAccuracy" FLOAT,
        "NavType" FLOAT,
        "OtherData" FLOAT,
        "Habitat" FLOAT,
        "Substrate" FLOAT,
        "CMECSGeoForm" FLOAT,
        "CMECSSubstrate" FLOAT,
        "CMECSBiotic" FLOAT,
        "Temperature" INTEGER,
        "Salinity" INTEGER,
        "Oxygen" INTEGER,
        "pH" INTEGER,
        "pHscale" FLOAT,
        "pCO2" INTEGER,
        "TA" INTEGER,
        "DIC" INTEGER,
        "RecordType" TEXT,
        "ImageFilePath" FLOAT,
        "HighlightImageFilePath" FLOAT,
        "DataProvider" TEXT,
        "DataContact" TEXT,
        "Modified" TEXT,
        "EntryUpdate" TEXT,
        "WebSite" FLOAT,
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

    # Execute the query using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.run(create_table_query)

def load_csv_to_postgres():
    # Load CSV and clean data
    csv_path = '/mnt/data/DSCRTP_NatDB_20241022-1_toprows.csv'
    duckdb_query = """
    SELECT *
    FROM read_csv_auto(?, header=True, na_strings=['NA', '-999'])
    """
    df = duckdb.query(duckdb_query, [csv_path]).to_df()

    # Insert data into PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.insert_rows('dscrtp_data', df.values.tolist(), target_fields=df.columns.tolist())

# Define tasks
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)

create_table_task >> load_csv_task
