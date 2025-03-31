from datetime import datetime, timedelta
import requests
import zipfile
import os
import tempfile
import subprocess
import logging
import netCDF4
from netCDF4 import Dataset


url = "https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024_tid/zip/"

zip_path = "./gebcodata/gebco_2024.zip"

response = requests.get(url, stream=True)
response.raise_for_status()

with open(zip_path, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall("./gebcodata")

file_path = "gebcodata/GEBCO_2024_TID.nc"
data = Dataset(file_path, 'r')
print(data)
latitudes = data.variables['lat'][:]
longitudes = data.variables['lon'][:]
TIDs = data.variables['tid'][:]

import rasterio
import rasterio.features
import geopandas as gpd
from shapely.geometry import shape

# Path to the input raster file
raster_path = 'gebcodata/GEBCO_2024_TID.nc'
# Path to the output shapefile
shapefile_path = 'output.shp'

with rasterio.open(raster_path) as src:
    image = src.read(1)
    results = (
        {'properties': {'raster_val': v}, 'geometry': shape(s)}
        for i, (s, v) in enumerate(rasterio.features.shapes(image, mask=None, transform=src.transform))
    )

geoms = list(results)
gdf = gpd.GeoDataFrame.from_features(geoms)
gdf.crs = src.crs
gdf.to_file(shapefile_path)

print(f"Shapefile saved to {shapefile_path}")

#print(f"{TIDs[1:4]}")

#os.remove(zip_path)  # Cleanup zip file
