import os
import requests
import zipfile
import geopandas as gpd
import h3

def download_osm_water_polygons(url, output_folder):
    """Downloads and extracts the OSM water polygons shapefile."""
    zip_filename = os.path.join(output_folder, "water-polygons.zip")
    extracted_folder = os.path.join(output_folder, "water-polygons")
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    print("Downloading OSM water polygons...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
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

    # Load the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path, rows=20) # remove row limitation later

    print(gdf)  # Print the GeoDataFrame directly

def identify_water_hexes(gdf):
    """Determines all hexagons that intersect water polygons."""
    
    waterhexes = set()
    
    for geom in gdf.geometry:
        geojson = geom.__geo_interface__  # Convert to GeoJSON format
        hexes = h3.geo_to_cells(geojson, 5)  # Adjust resolution as needed
        waterhexes.update(hexes)
    
    return waterhexes

if __name__ == "__main__":
    WATER_POLYGON_URL = "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip"
    OUTPUT_FOLDER = "osm_water_data"
    
    #download_osm_water_polygons(WATER_POLYGON_URL, OUTPUT_FOLDER)
    
    gdf = load_water_polygons("./osm_water_data/water-polygons/water-polygons-split-3857/water_polygons.shp")

    waterhexes = identify_water_hexes(gdf)