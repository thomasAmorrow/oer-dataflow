from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
import geopandas as gpd
import h3
import csv
import time

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
    """Downloads and extracts the OSM water polygons shapefile."""
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
        return
    
    # Extract the ZIP file
    print("Extracting files...")
    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        zip_ref.extractall(extracted_folder)
    print(f"Files extracted to {extracted_folder}")

def load_water_polygons(shapefile_path):
    """Loads OSM water polygons shapefile."""
    print("Loading shapefiles...")
    gdf = gpd.read_file(shapefile_path, rows=1000)
    print("...done")
    
    # Reproject to WGS 84 if the CRS isn't already EPSG:4326
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs(epsg=4326)
        print("Reprojected GeoDataFrame to EPSG:4326")

    return gdf

def process_geometry(geom):
    """Process a single geometry and return its H3 hexagons."""
    geojson = geom.__geo_interface__  # Convert the geometry to GeoJSON format
    hexes = h3.geo_to_cells(geojson, 5)  # Adjust resolution as needed
    return hexes

def identify_water_hexes(gdf):
    """Determines all hexagons that intersect water polygons sequentially."""
    waterhexes = set()  # Initialize an empty set to store unique hexes
    
    # Process each geometry sequentially
    for geom in gdf.geometry:
        hexes = process_geometry(geom)  # Process the geometry and get hexagons
        waterhexes.update(hexes)  # Update the waterhexes set with the result

    return waterhexes

def write_waterhexes_to_file(waterhexes, filename):
    """Write the set of water hexagons to a CSV file."""
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["H3_Index"])  # Header row
        for hexagon in waterhexes:
            writer.writerow([hexagon])  # Write each hexagon index
    print(f"Water hexagons saved to {filename}")

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
SHAPEFILE_PATH = f"{OUTPUT_FOLDER}/water-polygons/water-polygons-split-3857/water_polygons.shp"
OUTPUT_CSV = "/tmp/water_hexagons.csv"

# Task 1: Download and unzip OSM water polygons
download_task = PythonOperator(
    task_id='download_and_unzip_osm_water_polygons',
    python_callable=download_osm_water_polygons,
    op_kwargs={'url': WATER_POLYGON_URL, 'output_folder': OUTPUT_FOLDER},
    dag=dag,
)

# Task 2: Load OSM water polygons shapefile
load_task = PythonOperator(
    task_id='load_osm_water_polygons',
    python_callable=load_water_polygons,
    op_kwargs={'shapefile_path': SHAPEFILE_PATH},
    dag=dag,
)

# Task 3: Identify water hexagons from the shapefile
identify_task = PythonOperator(
    task_id='identify_water_hexagons',
    python_callable=identify_water_hexes,
    op_kwargs={'gdf': "{{ task_instance.xcom_pull(task_ids='load_osm_water_polygons') }}"},
    dag=dag,
)

# Task 4: Write identified water hexagons to CSV
write_task = PythonOperator(
    task_id='write_water_hexagons_to_csv',
    python_callable=write_waterhexes_to_file,
    op_kwargs={'waterhexes': "{{ task_instance.xcom_pull(task_ids='identify_water_hexagons') }}", 'filename': OUTPUT_CSV},
    dag=dag,
)

# Define task dependencies
download_task >> load_task >> identify_task >> write_task
