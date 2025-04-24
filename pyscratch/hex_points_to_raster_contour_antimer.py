import os
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.features import rasterize
from scipy.interpolate import griddata
from scipy.spatial import cKDTree
from shapely.affinity import translate
import zipfile
import urllib.request
import pandas as pd
from PIL import Image

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

def create_land_mask(x_mesh, y_mesh, land_shapefile_path, transform):
    land = gpd.read_file(land_shapefile_path).to_crs(epsg=3857)
    land_polygons = land.geometry

    shape = (len(y_mesh), len(x_mesh[0]))  # rows x cols
    land_mask_raster = rasterize(
        ((geom, 1) for geom in land_polygons),
        out_shape=shape,
        transform=transform,
        fill=0,
        dtype=np.uint8
    )
    ocean_mask = land_mask_raster == 0
    return ocean_mask

# === Setup ===
shapefile_path = download_land_shapefile("app/shape")
gdf = gpd.read_file("app/geo/h3_points_03.geojson")

# Reproject to EPSG:3857
gdf_3857 = gdf.to_crs(epsg=3857)

# Duplicate near-antimeridian points
x_wrap_threshold = 2.00375e7
right_wrap = gdf_3857[gdf_3857.geometry.x < -x_wrap_threshold + 1e5].copy()
left_wrap = gdf_3857[gdf_3857.geometry.x > x_wrap_threshold - 1e5].copy()
right_wrap['geometry'] = right_wrap['geometry'].translate(xoff=2 * x_wrap_threshold)
left_wrap['geometry'] = left_wrap['geometry'].translate(xoff=-2 * x_wrap_threshold)

gdf_3857_wrapped = gpd.GeoDataFrame(pd.concat([gdf_3857, left_wrap, right_wrap]), crs=gdf_3857.crs)

# Extract values
x = gdf_3857_wrapped.geometry.x.values
y = gdf_3857_wrapped.geometry.y.values
values = gdf_3857_wrapped["combined"].values

# Grid definition in Web Mercator
x_grid = np.linspace(-2.00375e7, 2.00375e7, 8192)
y_grid = np.linspace(2.00375e7, -2.00375e7, 8192)
x_mesh, y_mesh = np.meshgrid(x_grid, y_grid)

# Interpolation
grid_values = griddata(
    points=(x, y),
    values=values,
    xi=(x_mesh, y_mesh),
    method='cubic'
)

# Distance-based mask
tree = cKDTree(np.c_[x, y])
distances, _ = tree.query(np.c_[x_mesh.ravel(), y_mesh.ravel()], k=1)
distance_mask = distances.reshape(y_mesh.shape) > 300000  # 300 km in meters

# Land mask
transform = from_origin(-2.00375e7, 2.00375e7, x_grid[1] - x_grid[0], y_grid[0] - y_grid[1])
land_mask = create_land_mask(x_mesh, y_mesh, shapefile_path, transform)

# Combine masks
combined_mask = np.logical_or(distance_mask, ~land_mask)
grid_values = np.ma.masked_where(combined_mask, grid_values)

# === Normalize to 8-bit range for tiling ===
if np.ma.is_masked(grid_values):
    valid_data = grid_values.filled(np.nan)  # Fill masked values with NaN
else:
    valid_data = grid_values

# Now, calculate min and max values, excluding NaN values
min_val = np.nanmin(valid_data)
max_val = np.nanmax(valid_data)

# Debugging print of valid range
print(f"Valid value range: min = {min_val:.3f}, max = {max_val:.3f}")

# If all values are equal, set the scaled values to zero
if max_val == min_val:
    print("Warning: all values are equal.")
    scaled_values = np.zeros_like(grid_values, dtype=np.uint8)
else:
    # Normalize the values between 0 and 1
    scaled = (grid_values - min_val) / (max_val - min_val)
    
    # Debug print of scaled range
    print("Scaled value range:", scaled.min(), scaled.max())

    # Scale to 8-bit (0 to 255)
    scaled_values = (scaled * 255).astype(np.uint8)

    # Mask the invalid (masked) values to 0
    scaled_values = np.where(grid_values.mask, 0, scaled_values)

# === Save 8-bit GeoTIFF for tiling with Transparency ===
output_tiff_8bit = "ocean_contours_3857_8bit_transparent.tif"
metadata_8bit = {
    'driver': 'GTiff',
    'count': 1,
    'dtype': 'uint8',
    'crs': 'EPSG:3857',
    'width': len(x_grid),
    'height': len(y_grid),
    'nodata': 0,  # Transparency is handled by setting 'nodata' to 0
    'transform': transform
}

# Write the 8-bit GeoTIFF with transparency
with rasterio.open(output_tiff_8bit, 'w', **metadata_8bit) as dst:
    dst.write(scaled_values, 1)

print(f"8-bit GeoTIFF with transparency saved as {output_tiff_8bit}")
