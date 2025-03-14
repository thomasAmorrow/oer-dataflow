from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import zipfile
import geopandas as gpd
import h3
import csv
import json

def download_osm_water_polygons(url, output_folder):
    """Downloads the OSM water polygons shapefile."""
    zip_filename = os.path.join(output_folder, "water-polygons.zip")
    
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
        raise Exception("Download failed.")
    
    return zip_filename


def extract_zip(zip_filename, output_folder):
    """Extract the downloaded ZIP file."""
    extracted_folder = os.path.join(output_folder, "water-polygons")
    print(f"Extracting files to {extracted_folder}...")
    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        zip_ref.extractall(extracted_folder)
    print("Extraction complete.")
    return extracted_folder


def load_water_polygons(shapefile_path):
    """Loads the OSM water polygons shapefile and serializes it to GeoJSON."""
    print(f"Loading shapefile from {shapefile_path}...")
    gdf = gpd.read_file(shapefile_path, rows=1000)
    print("Shapefile loaded.")
    
    # Reproject to WGS 84 if the CRS isn't already EPSG:4326
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs(epsg=4326)
        print("Reprojected GeoDataFrame to EPSG:4326")
    
    # Serialize the GeoDataFrame to GeoJSON
    geojson = gdf.to_json()
    return geojson


def process_geometry(geom):
    """Process a single geometry and return its H3 hexagons."""
    geojson = geom.__geo_interface__  # Convert the geometry to GeoJSON format
    hexes = h3.geo_to_cells(geojson, 5)  # Adjust resolution as needed
    return hexes


def identify_water_hexes(geojson_data):
    """Identifies all hexagons that intersect water polygons."""
    # Deserialize GeoJSON to GeoDataFrame
    gdf = gpd.read_file(json.loads(geojson_data))
    
    waterhexes = set()  # Initialize an empty set to store unique hexes
    
    # Loop through all geometries in the GeoDataFrame
    for geom in gdf.geometry:
        hexes = process_geometry(geom)  # Process the geometry and get hexagons
        waterhexes.update(hexes)  # Update the waterhexes set with the result

    return list(waterhexes)


def write_waterhexes_to_file(waterhexes, filename):
    """Write the set of water hexagons to a CSV file."""
    print(f"Writing water hexagons to {filename}...")
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["H3_Index"])  # Header row
        for hexagon in waterhexes:
            writer.writerow([hexagon])  # Write each hexagon index
    print(f"Water hexagons saved to {filename}")


# Define the default_args dictionary for Airflow DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'dataset_create_hex_heirarchy_from_water_polygons',
    default_args=default_args,
    description='Download, process, and save OSM water hexagons',
    schedule_interval=None,  # Change this to your preferred schedule (e.g., '@daily')
    start_date=datetime(2025, 3, 13),  # Set your desired start date
    catchup=False,
)

# Define the tasks in the DAG
download_task = PythonOperator(
    task_id='download_osm_water_polygons',
    python_callable=download_osm_water_polygons,
    op_args=["https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip", "osm_water_data"],
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_zip',
    python_callable=extract_zip,
    op_args=["{{ task_instance.xcom_pull(task_ids='download_osm_water_polygons') }}", "osm_water_data"],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_water_polygons',
    python_callable=load_water_polygons,
    op_args=["{{ task_instance.xcom_pull(task_ids='extract_zip') }}/water-polygons-split-3857/water_polygons.shp"],
    dag=dag,
)

identify_task = PythonOperator(
    task_id='identify_water_hexes',
    python_callable=identify_water_hexes,
    op_args=["{{ task_instance.xcom_pull(task_ids='load_water_polygons') }}"],  # Pass the serialized GeoDataFrame
    dag=dag,
)

write_task = PythonOperator(
    task_id='write_waterhexes_to_file',
    python_callable=write_waterhexes_to_file,
    op_args=["{{ task_instance.xcom_pull(task_ids='identify_water_hexes') }}", "water_hexagons.csv"],
    dag=dag,
)

# Set task dependencies
download_task >> extract_task >> load_task >> identify_task >> write_task
