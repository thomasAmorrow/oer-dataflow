import os
import requests
import zipfile
import geopandas as gpd
import h3
import csv
import time
from tqdm import tqdm  # Import tqdm for progress bar

def download_osm_water_polygons(url, output_folder):
    """Downloads and extracts the OSM water polygons shapefile with a progress bar."""
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
            # Use tqdm to show download progress
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading OSM data") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    pbar.update(len(chunk))  # Update the progress bar
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
    # Load the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path, rows=1000)
    
    # Reproject to WGS 84 if the CRS isn't already EPSG:4326
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs(epsg=4326)
        print("Reprojected GeoDataFrame to EPSG:4326")

    return gdf

def identify_water_hexes(gdf):
    """Determines all hexagons that intersect water polygons with a progress bar."""
    
    waterhexes = set()  # Initialize an empty set to store unique hexes
    
    # Use tqdm to add a progress bar for the loop over geometries
    with tqdm(total=len(gdf.geometry), desc="Identifying water hexagons", unit="geometry") as pbar:
        for geom in gdf.geometry:
            geojson = geom.__geo_interface__  # Convert the geometry to GeoJSON format
            hexes = h3.geo_to_cells(geojson, 5)  # Adjust resolution as needed
            waterhexes.update(hexes)
            pbar.update(1)  # Update progress bar after each geometry
            
    return waterhexes

def write_waterhexes_to_file(waterhexes, filename):
    """Write the set of water hexagons to a CSV file."""
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["H3_Index"])  # Header row
        for hexagon in waterhexes:
            writer.writerow([hexagon])  # Write each hexagon index
    print(f"Water hexagons saved to {filename}")

if __name__ == "__main__":
    start_time = time.time()  # Record the start time
    
    WATER_POLYGON_URL = "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip"
    OUTPUT_FOLDER = "osm_water_data"
    
    # Download the shapefile with progress bar
    #download_osm_water_polygons(WATER_POLYGON_URL, OUTPUT_FOLDER)
    
    # Load the water polygons shapefile
    gdf = load_water_polygons("./osm_water_data/water-polygons/water-polygons-split-3857/water_polygons.shp")

    # Identify the water hexagons with progress bar
    waterhexes = identify_water_hexes(gdf)

    # Save the water hexagons to a CSV file
    write_waterhexes_to_file(waterhexes, "water_hexagons.csv")
    
    # Calculate the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Process completed in {elapsed_time:.2f} seconds.")
