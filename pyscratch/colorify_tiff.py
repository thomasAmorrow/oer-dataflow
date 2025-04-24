import numpy as np
import rasterio
import matplotlib.pyplot as plt
from matplotlib import colors
import os

# Path to your input GeoTIFF
input_tiff = 'temp.vrt'  # Change this to your input VRT or GeoTIFF path
output_tiff = 'viridis_global_ocean_contours_3857.tif'  # Output path for the colormap-applied GeoTIFF

# Define the colormap and normalization
cmap = plt.cm.viridis  # Choose colormap (e.g., viridis, plasma, etc.)
norm = colors.Normalize(vmin=0, vmax=100)  # Adjust based on your data

# Read the GeoTIFF
with rasterio.open(input_tiff) as src:
    data = src.read(1)  # First band
    transform = src.transform
    crs = src.crs
    width, height = src.width, src.height

    # Apply colormap
    rgba_image = cmap(norm(data))
    rgb = (rgba_image[:, :, :3] * 255).astype(np.uint8)

    # Create alpha channel: 0 if value == 0, else 255
    alpha = np.where(data == 0, 0, 255).astype(np.uint8)

    # Write RGBA GeoTIFF
    with rasterio.open(
        output_tiff, 'w',
        driver='GTiff',
        height=height,
        width=width,
        count=4,  # Red, Green, Blue, Alpha
        dtype='uint8',
        crs=crs,
        transform=transform
    ) as dst:
        dst.write(rgb[:, :, 0], 1)  # Red
        dst.write(rgb[:, :, 1], 2)  # Green
        dst.write(rgb[:, :, 2], 3)  # Blue
        dst.write(alpha, 4)         # Alpha

print(f"Colormapped RGBA GeoTIFF saved to: {output_tiff}")
