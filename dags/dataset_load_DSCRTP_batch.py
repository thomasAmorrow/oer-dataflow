from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Function to load data in batches
def load_csv_to_postgres():
    # CSV file path
    csv_path = '/tmp/DSCRTP_NatDB_cleaned.csv'
    batch_size = 3  # Adjust batch size based on your system's capability
    
    # PostgreSQL connection
    postgres_hook = PostgresHook(postgres_conn_id='oceexp-db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(reader)  # Skip the header row
        
        batch = []
        for row in reader:
            # Skip the first 3 columns
            row = row[3:]
            print(len(row))
            print(row)
            # Append the cleaned row to the batch
            batch.append(tuple(row))
            print(batch)
            
            # Insert batch into the database when the batch size is reached
            if len(batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO dscrtp (
                        "ShallowFlag", "DatabaseVersion", "DatasetID", "CatalogNumber", "SampleID", "TrackingID", 
                        "ImageURL", "HighlightImageURL", "Citation", "Repository", "ScientificName", "VerbatimScientificName", 
                        "VernacularNameCategory", "VernacularName", "TaxonRank", "AphiaID", "LifeScienceIdentifier", "Phylum", "Class", 
                        "Subclass", "Order", "Suborder", "Family", "Subfamily", "Genus", "Subgenus", "Species", "Subspecies", 
                        "ScientificNameAuthorship", "TypeStatus", "OperationalTaxonomicUnit", "Morphospecies", "CombinedNameID", "Synonyms", 
                        "IdentificationComments", "IdentifiedBy", "IdentificationDate", "IdentificationQualifier", "IdentificationVerificationStatus", 
                        "AssociatedSequences", "Ocean", "LargeMarineEcosystem", "Country", "FishCouncilRegion", "Locality", "Latitude", 
                        "Longitude", "DepthInMeters", "DepthMethod", "MinimumDepthInMeters", "MaximumDepthInMeters", "LocationComments", 
                        "ObservationDate", "ObservationYear", "ObservationTime", "SurveyID", "Vessel", "PI", "PIAffiliation", "Purpose", 
                        "SurveyComments", "Station", "EventID", "SamplingEquipment", "VehicleName", "SampleAreaInSquareMeters", "footprintWKT", 
                        "footprintSRS", "IndividualCount", "CategoricalAbundance", "Density", "Cover", "VerbatimSize", "MinimumSize", 
                        "MaximumSize", "WeightInKg", "Condition", "AssociatedTaxa", "OccurrenceComments", "StartLatitude", "StartLongitude", 
                        "EndLatitude", "EndLongitude", "VerbatimLatitude", "VerbatimLongitude", "LocationAccuracy", "NavType", "OtherData", 
                        "Habitat", "Substrate", "CMECSGeoForm", "CMECSSubstrate", "CMECSBiotic", "Temperature", "Salinity", "Oxygen", "pH", 
                        "pHscale", "pCO2", "TA", "DIC", "RecordType", "ImageFilePath", "HighlightImageFilePath", "DataProvider", "DataContact", 
                        "Modified", "EntryUpdate", "WebSite", "EntryDate", "Reporter", "ReporterEmail", "ReporterComments", "AccessionID", 
                        "gisLandCheck", "gisCRMDepth", "gisGEBCODepth", "gisEtopoDepth", "gisNGIALocality", "gisGEBCOLocality", "gisNGIADist", 
                        "gisGEBCODist", "gisLME", "gisMEOW", "gisIHOSeas", "gisCaribbeanFMC", "gisGulfOfMexicoFMC", "gisMidAtlanticFMC", 
                        "gisNewEnglandFMC", "gisNorthPacificFMC", "gisPacificFMC", "gisSouthAtlanticFMC", "gisWesternPacificFMC"
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()
                batch = []  # Reset the batch
        
        # Insert any remaining rows
        if batch:
            cursor.executemany("""
                (Repeat the same INSERT query above)
            """, batch)
            conn.commit()

    cursor.close()
    conn.close()

with DAG(
    dag_id='dataset_load_DSCRTP_batch',
    default_args=default_args,
    description='Load DSCRTP CSV into PostgreSQL in batches',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )
