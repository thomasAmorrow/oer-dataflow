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
    CREATE TABLE IF NOT EXISTS dscrtp (
    "ShallowFlag" BOOLEAN,
    "DatabaseVersion" VARCHAR,
    "DatasetID" VARCHAR,
    "CatalogNumber" INTEGER,
    "SampleID" INTEGER,
    "TrackingID" VARCHAR,
    "ImageURL" VARCHAR,
    "HighlightImageURL" VARCHAR,
    "Citation" VARCHAR,
    "Repository" VARCHAR,
    "ScientificName" VARCHAR,
    "VerbatimScientificName" VARCHAR,
    "VernacularNameCategory" VARCHAR,
    "VernacularName" VARCHAR,
    "TaxonRank" VARCHAR,
    "AphiaID" INTEGER,
    "LifeScienceIdentifier" VARCHAR,
    "Phylum" VARCHAR,
    "Class" VARCHAR,
    "Subclass" VARCHAR,
    "Order" VARCHAR,
    "Suborder" VARCHAR,
    "Family" VARCHAR,
    "Subfamily" VARCHAR,
    "Genus" VARCHAR,
    "Subgenus" VARCHAR,
    "Species" VARCHAR,
    "Subspecies" VARCHAR,
    "ScientificNameAuthorship" VARCHAR,
    "TypeStatus" VARCHAR,
    "OperationalTaxonomicUnit" VARCHAR,
    "Morphospecies" VARCHAR,
    "CombinedNameID" INTEGER,
    "Synonyms" VARCHAR,
    "IdentificationComments" TEXT,
    "IdentifiedBy" VARCHAR,
    "IdentificationDate" DATE,
    "IdentificationQualifier" VARCHAR,
    "IdentificationVerificationStatus" VARCHAR,
    "AssociatedSequences" TEXT,
    "Ocean" VARCHAR,
    "LargeMarineEcosystem" VARCHAR,
    "Country" VARCHAR,
    "FishCouncilRegion" VARCHAR,
    "Locality" VARCHAR,
    "Latitude" FLOAT,
    "Longitude" FLOAT,
    "DepthInMeters" FLOAT,
    "DepthMethod" VARCHAR,
    "MinimumDepthInMeters" FLOAT,
    "MaximumDepthInMeters" FLOAT,
    "LocationComments" TEXT,
    "ObservationDate" DATE,
    "ObservationYear" INTEGER,
    "ObservationTime" TIME,
    "SurveyID" VARCHAR,
    "Vessel" VARCHAR,
    "PI" VARCHAR,
    "PIAffiliation" VARCHAR,
    "Purpose" VARCHAR,
    "SurveyComments" TEXT,
    "Station" VARCHAR,
    "EventID" VARCHAR,
    "SamplingEquipment" VARCHAR,
    "VehicleName" VARCHAR,
    "SampleAreaInSquareMeters" FLOAT,
    "footprintWKT" TEXT,
    "footprintSRS" VARCHAR,
    "IndividualCount" INTEGER,
    "CategoricalAbundance" VARCHAR,
    "Density" FLOAT,
    "Cover" FLOAT,
    "VerbatimSize" VARCHAR,
    "MinimumSize" FLOAT,
    "MaximumSize" FLOAT,
    "WeightInKg" FLOAT,
    "Condition" VARCHAR,
    "AssociatedTaxa" TEXT,
    "OccurrenceComments" TEXT,
    "StartLatitude" FLOAT,
    "StartLongitude" FLOAT,
    "EndLatitude" FLOAT,
    "EndLongitude" FLOAT,
    "VerbatimLatitude" FLOAT,
    "VerbatimLongitude" FLOAT,
    "LocationAccuracy" FLOAT,
    "NavType" VARCHAR,
    "OtherData" TEXT,
    "Habitat" VARCHAR,
    "Substrate" VARCHAR,
    "CMECSGeoForm" VARCHAR,
    "CMECSSubstrate" VARCHAR,
    "CMECSBiotic" VARCHAR,
    "Temperature" FLOAT,
    "Salinity" FLOAT,
    "Oxygen" FLOAT,
    "pH" FLOAT,
    "pHscale" VARCHAR,
    "pCO2" FLOAT,
    "TA" FLOAT,
    "DIC" FLOAT,
    "RecordType" VARCHAR,
    "ImageFilePath" VARCHAR,
    "HighlightImageFilePath" VARCHAR,
    "DataProvider" VARCHAR,
    "DataContact" VARCHAR,
    "Modified" DATE,
    "EntryUpdate" DATE,
    "WebSite" VARCHAR,
    "EntryDate" DATE,
    "Reporter" VARCHAR,
    "ReporterEmail" VARCHAR,
    "ReporterComments" TEXT,
    "AccessionID" VARCHAR,
    "gisLandCheck" BOOLEAN,
    "gisCRMDepth" FLOAT,
    "gisGEBCODepth" FLOAT,
    "gisEtopoDepth" FLOAT,
    "gisNGIALocality" VARCHAR,
    "gisGEBCOLocality" VARCHAR,
    "gisNGIADist" FLOAT,
    "gisGEBCODist" FLOAT,
    "gisLME" VARCHAR,
    "gisMEOW" VARCHAR,
    "gisIHOSeas" VARCHAR,
    "gisCaribbeanFMC" VARCHAR,
    "gisGulfOfMexicoFMC" VARCHAR,
    "gisMidAtlanticFMC" VARCHAR,
    "gisNewEnglandFMC" VARCHAR,
    "gisNorthPacificFMC" VARCHAR,
    "gisPacificFMC" VARCHAR,
    "gisSouthAtlanticFMC" VARCHAR,
    "gisWesternPacificFMC" VARCHAR
);
    """
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    postgres_hook.run(create_table_query)

def load_csv_to_postgres():
    # Specify the CSV file path (it must be accessible to the PostgreSQL server)
    csv_path = '/tmp/DSCRTP_NatDB.csv'

    # COPY command to load CSV data into PostgreSQL
    copy_query = f"""
    COPY dscrtp_data FROM '{csv_path}' WITH CSV HEADER DELIMITER ',' NULL 'NA';
    """
    
    # PostgreSQL hook to run the COPY query
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
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
