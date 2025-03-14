from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
import geopandas as gpd
import h3
import csv

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def download_osm_water_polygons(url, output_folder):
    """Downloads and extracts OSM water polygons zip file."""
    zip_filename = os.path.join(output_folder, "water-polygons.zip")
    extracted_folder = os.path.join(output_folder, "water-polygons")
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    print("Downloading OSM water polygons...")
    response = requests.get(url, stream=True)
    
    # Check for successful response
    if response.status_code == 200:
        total_size = int(response.headers.get('Content-Length', 0))  # Get total file size
        with open(zip_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print("Download complete.")
    else:
        print("Failed to download the file.")
        return None, None
    
    # Extract the ZIP file
    print("Extracting files...")
    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        zip_ref.extractall(extracted_folder)
    print(f"Files extracted to {extracted_folder}")

    return zip_filename, extracted_folder

def process_and_identify_hexagons(extracted_folder, output_csv):
    """Loads the shapefile, processes geometry, and saves hexagons to CSV."""
    shapefile_path = os.path.join(extracted_folder, "water-polygons/water-polygons-split-3857", "water_polygons.shp") # this could be more efficiently named/handled
    
    # Load the shapefile into a GeoDataFrame
    print("Loading shapefiles from ")
    print(shapefile_path)
    gdf = gpd.read_file(shapefile_path, rows=1000)
    print("...done")
    
    # Reproject to WGS 84 if the CRS isn't already EPSG:4326
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs(epsg=4326)
        print("Reprojected GeoDataFrame to EPSG:4326")

    # Identify water hexagons and save to CSV
    waterhexes = set()  # Initialize an empty set to store unique hexes
    for geom in gdf.geometry:
        geojson = geom.__geo_interface__  # Convert the geometry to GeoJSON format
        hexes = h3.geo_to_cells(geojson, 5)  # Adjust resolution as needed
        waterhexes.update(hexes)  # Update the waterhexes set with the result
    
    # Write identified water hexagons to CSV
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["H3_Index"])  # Header row
        for hexagon in waterhexes:
            writer.writerow([hexagon])  # Write each hexagon index
    print(f"Water hexagons saved to {output_csv}")

# Define the DAG
dag = DAG(
    'dataset_create_hex_heirarchy_from_water_polygons',
    default_args=default_args,
    description='DAG to download, process, and save OSM water hexagons',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Define task arguments
WATER_POLYGON_URL = "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip"
OUTPUT_FOLDER = "/tmp/osm_water_data"
OUTPUT_CSV = "/tmp/water_hexagons.csv"

# Task 1: Download and unzip OSM water polygons
download_task = PythonOperator(
    task_id='download_osm_water_polygons',
    python_callable=download_osm_water_polygons,
    op_kwargs={'url': WATER_POLYGON_URL, 'output_folder': OUTPUT_FOLDER},
    dag=dag,
)

# Task 2: Process shapefile and identify hexagons
process_task = PythonOperator(
    task_id='process_and_identify_hexagons',
    python_callable=process_and_identify_hexagons,
    op_kwargs={'extracted_folder': OUTPUT_FOLDER, 'output_csv': OUTPUT_CSV},
    dag=dag,
)

# Set task dependencies
download_task >> process_task
