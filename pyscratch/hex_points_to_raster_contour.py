import os
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.features import rasterize  # Add this import
from rasterio.enums import Resampling
from scipy.interpolate import griddata
from scipy.spatial import cKDTree
from shapely.geometry import Point
from shapely.strtree import STRtree
import zipfile
import urllib.request

def download_land_shapefile(dest_folder):
    url = "https://naciscdn.org/naturalearth/110m/physical/ne_110m_land.zip"
    zip_path = os.path.join(dest_folder, "ne_110m_land.zip")

    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    if not os.path.exists(zip_path):
        print("Downloading 1:110m land shapefile...")
        urllib.request.urlretrieve(url, zip_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(dest_folder)
    print("Shapefile extracted to:", dest_folder)

    return os.path.join(dest_folder, "ne_110m_land.shp")


def create_land_mask(lon_mesh, lat_mesh, land_shapefile_path):
    """
    Creates a mask array where land is False and ocean is True.
    Inputs:
        lon_mesh, lat_mesh: 2D arrays from np.meshgrid
        land_shapefile_path: path to a land polygon shapefile
    Returns:
        ocean_mask: boolean 2D array where True = ocean, False = land
    """
    land = gpd.read_file(land_shapefile_path).to_crs("EPSG:4326")
    land_polygons = land.geometry

    # Create the bounds for the grid
    transform = rasterio.transform.from_origin(-180, 90, 360/len(lon_mesh[0]), 180/len(lat_mesh))
    shape = (len(lat_mesh), len(lon_mesh[0]))  # Number of rows x columns

    # Rasterize the land polygons (1 = land, 0 = ocean)
    land_mask_raster = rasterize(
        ((geom, 1) for geom in land_polygons), out_shape=shape, transform=transform, fill=0, dtype=np.uint8
    )

    # Invert the raster (flip upside down to match latitudes)
    #land_mask_raster = np.flipud(land_mask_raster)  # Flipping the array

    # Convert the raster into a boolean mask: True for ocean, False for land
    ocean_mask = land_mask_raster == 0  # Ocean is marked as 0, land as 1

    return ocean_mask


# === Setup and Download Shapefile ===
shapefile_path = download_land_shapefile("app/shape")

# === Load GeoJSON ===
gdf = gpd.read_file("app/geo/h3_points_03.geojson")
lons = gdf.geometry.x.values
lats = gdf.geometry.y.values
values = gdf["combined"].values

# === Define Global Grid ===
lon_grid = np.linspace(-180, 180, 8192)
lat_grid = np.linspace(90, -90, 4096)
lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)

# === Interpolate ===
grid_values = griddata(
    points=(lons, lats),
    values=values,
    xi=(lon_mesh, lat_mesh),
    method='cubic'
)

# === Distance Mask to Limit to Ocean Area ===
tree = cKDTree(np.c_[lons, lats])
distances, _ = tree.query(np.c_[lon_mesh.ravel(), lat_mesh.ravel()], k=1)
distance_threshold_deg = 3.0  # ~300 km
distance_mask = distances.reshape(lat_mesh.shape) > distance_threshold_deg

# === Land Mask ===
land_mask = create_land_mask(lon_mesh, lat_mesh, shapefile_path)

# === Combine Masks ===
combined_mask = np.logical_or(distance_mask, ~land_mask)
grid_values = np.ma.masked_where(combined_mask, grid_values)

# === Save as GeoTIFF ===
output_tiff = "global_ocean_contours.tif"

# Define the GeoTIFF metadata and transformation
transform = from_origin(-180, 90, 360/len(lon_mesh[0]), 180/len(lat_mesh))
metadata = {
    'driver': 'GTiff',
    'count': 1,
    'dtype': 'float32',
    'crs': 'EPSG:4326',
    'width': len(lon_mesh[0]),
    'height': len(lat_mesh),
    'nodata': np.nan,
    'transform': transform
}

# Write the GeoTIFF
with rasterio.open(output_tiff, 'w', **metadata) as dst:
    dst.write(grid_values.filled(np.nan), 1)

print(f"GeoTIFF saved as {output_tiff}")

