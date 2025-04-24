import os
import geopandas as gpd
import mercantile
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm

# Paths to the GeoJSON files
geojson_paths = {
    '03': 'app/geo/h3_hexagons_03_closed.geojson',
    '04': 'app/geo/h3_hexagons_04_closed.geojson',
    '05': 'app/geo/h3_hexagons_05_closed.geojson'
}

# Define the zoom levels
zoom_levels = {
    '03': (3, 6),
    '04': (7, 9),
    '05': (10, 12)
}

# Ensure the root tiles directory exists
os.makedirs("tiles", exist_ok=True)

# Create directory structure for each zoom level
for zoom in zoom_levels:
    os.makedirs(f"tiles/{zoom}", exist_ok=True)

# Function to generate tile image for a given zoom level
def generate_tile(zoom, x_tile, y_tile, gdf):
    # Get the bounding box of the tile at this zoom level
    bounds = mercantile.bounds(x_tile, y_tile, zoom)
    
    # Filter the GeoDataFrame by bounding box
    bbox = gdf.cx[bounds[0]:bounds[2], bounds[1]:bounds[3]]
    
    # Create the image grid
    width = 256  # Tile size in pixels
    height = 256
    img = np.zeros((height, width, 4), dtype=np.uint8)  # RGBA image

    # Plot hexagons into the image
    fig, ax = plt.subplots(figsize=(width / 100, height / 100), dpi=100)
    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    
    # Plot the hexagons
    for _, row in bbox.iterrows():
        hex_polygon = row['geometry']

        # Debug: Print the row to inspect its structure
        #print(row)

        # Safely access the properties
        properties = row.get('properties', {})
        combined_score = properties.get('combined', 0)  # Default to 0 if not found
        
        # You can change this to 'geology' or 'occurrence' if preferred
        color = cm.viridis(combined_score)  # Use a colormap (Viridis here)
        
        ax.fill(*hex_polygon.exterior.xy, color=color, alpha=0.7)

    # Remove axis and labels
    ax.axis('off')

    # Create the output directory for the tile if it does not exist
    tile_dir = f"tiles/{zoom}/{x_tile}_{y_tile}"
    os.makedirs(tile_dir, exist_ok=True)

    # Print the path to debug
    print(f"Saving tile to: {tile_dir}/tile.png")

    # Save the figure as PNG in the specific directory
    plt.savefig(f'{tile_dir}/tile.png', dpi=100, bbox_inches='tight', pad_inches=0)
    plt.close(fig)

# Generate tiles for each zoom level and tile coordinate
for zoom, (min_zoom, max_zoom) in zoom_levels.items():
    # Load the appropriate GeoJSON file for each zoom level
    gdf = gpd.read_file(geojson_paths[zoom])
    
    for zoom_level in range(min_zoom, max_zoom + 1):
        for x_tile in range(mercantile.tile(-180, 85, zoom_level)[0], mercantile.tile(180, -85, zoom_level)[0] + 1):
            for y_tile in range(mercantile.tile(-180, 85, zoom_level)[1], mercantile.tile(180, -85, zoom_level)[1] + 1):
                generate_tile(zoom_level, x_tile, y_tile, gdf)
