import geopandas as gpd
import matplotlib.pyplot as plt
import mercantile
import sqlite3
import io
from shapely.geometry import box, Polygon as ShapelyPolygon, LineString
from matplotlib.patches import Polygon as MplPolygon
from PIL import Image
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import threading
import traceback
import multiprocessing
import matplotlib.colors as mcolors
from shapely.ops import split
import shapely.affinity

# CONFIG
MIN_ZOOM_LEVEL = 0
MAX_ZOOM_LEVEL = 15
OUTPUT_MB_TILES_PATH = 'tiles/tiles.mbtiles'
TILE_SIZE = 256  # pixels

# Set up logging
logging.basicConfig(filename='process_errors.log', level=logging.ERROR)
db_semaphore = threading.Semaphore(1)

def get_geojson_for_zoom(zoom_level):
    if zoom_level < 7:
        return 'app/geo/h3_hexagons_03_closed.geojson'
    elif 7 <= zoom_level < 10:
        return 'app/geo/h3_hexagons_04_closed.geojson'
    elif zoom_level >= 10:
        return 'app/geo/h3_hexagons_05_closed.geojson'
    return None

def close_ring_if_needed(geom):
    if not geom.is_valid or not geom.exterior.is_ring:
        coords = list(geom.exterior.coords)
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        return ShapelyPolygon(coords)
    return geom

def latlon_bounds(tile):
    bbox = mercantile.bounds(tile)
    return box(bbox.west, bbox.south, bbox.east, bbox.north)

def split_antimeridian(geom):
    if geom.is_empty or geom.geom_type != "Polygon":
        return [geom]

    minx, _, maxx, _ = geom.bounds
    if minx >= -180 and maxx <= 180:
        return [geom]

    split_line = LineString([(180, -90), (180, 90)])
    try:
        split_polys = split(geom, split_line)
    except Exception:
        return [geom]

    result = []
    for poly in split_polys.geoms:
        pxmin, _, pxmax, _ = poly.bounds
        if pxmin > 180:
            poly = shapely.affinity.translate(poly, xoff=-360)
        elif pxmax < -180:
            poly = shapely.affinity.translate(poly, xoff=360)
        result.append(poly)
    return result

def preprocess_antimeridian_splits(gdf):
    new_rows = []
    for _, row in gdf.iterrows():
        parts = split_antimeridian(row.geometry)
        for geom in parts:
            new_row = row.copy()
            new_row.geometry = geom
            new_rows.append(new_row)
    return gpd.GeoDataFrame(new_rows, columns=gdf.columns, crs=gdf.crs)

def save_tile_to_mbtiles(tile, polygons, conn, zoom_level):
    fig, ax = plt.subplots(figsize=(TILE_SIZE / 100, TILE_SIZE / 100), dpi=100)

    if zoom_level in [0, 1, 2]:
        ax.set_facecolor((0, 0, 0, 0))
    else:
        bounds = mercantile.bounds(tile)
        ax.set_xlim(bounds.west, bounds.east)
        ax.set_ylim(bounds.south, bounds.north)
        ax.axis('off')

        norm = mcolors.Normalize(vmin=0, vmax=1)
        cmap = plt.get_cmap('viridis')

        for _, row in polygons.iterrows():
            geom = close_ring_if_needed(row.geometry)
            combined = row.get("combined", 0.0)
            color = cmap(norm(combined))
            alpha = max(0, 0.5 - combined * 0.5)

            patch = MplPolygon(
                list(geom.exterior.coords),
                facecolor=color,
                edgecolor='white',
                linewidth=0.5,
                alpha=alpha
            )
            ax.add_patch(patch)

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0, transparent=True)
    buf.seek(0)
    img_data = buf.read()
    buf.close()

    zoom, x, y = tile.z, tile.x, tile.y
    tile_row = (zoom, x, y, img_data)

    with db_semaphore:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)', tile_row
        )
        conn.commit()

    plt.close()

def create_mbtiles_database():
    conn = sqlite3.connect(OUTPUT_MB_TILES_PATH)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tiles (
        zoom_level INTEGER,
        tile_column INTEGER,
        tile_row INTEGER,
        tile_data BLOB,
        PRIMARY KEY (zoom_level, tile_column, tile_row)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        name TEXT,
        value TEXT
    )''')

    cursor.execute("INSERT INTO metadata (name, value) VALUES ('name', 'Raster Tiles')")
    cursor.execute("INSERT INTO metadata (name, value) VALUES ('type', 'baselayer')")
    cursor.execute("INSERT INTO metadata (name, value) VALUES ('version', '1.0.0')")
    cursor.execute("INSERT INTO metadata (name, value) VALUES ('format', 'png')")

    conn.commit()
    return conn

def render_tile_for_zoom_level(zoom_level, tile, gdf):
    try:
        conn = create_mbtiles_database()
        tile_bounds = latlon_bounds(tile)
        intersecting = gdf[gdf.intersects(tile_bounds)]

        if not intersecting.empty:
            save_tile_to_mbtiles(tile, intersecting, conn, zoom_level)

        conn.close()

    except Exception as e:
        logging.error(f"Error rendering tile {tile.z}/{tile.x}/{tile.y} at zoom level {zoom_level}: {str(e)}")
        logging.error(traceback.format_exc())

def render_tiles_for_zoom_level(zoom_level):
    geojson_file = get_geojson_for_zoom(zoom_level)
    if geojson_file is None:
        print(f"No GeoJSON file available for zoom level {zoom_level}. Skipping.")
        return

    gdf = gpd.read_file(geojson_file)
    gdf = gdf.to_crs(epsg=4326)
    gdf = preprocess_antimeridian_splits(gdf)

    tiles = list(mercantile.tiles(-180, -90, 180, 90, zoom_level))

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = [
            executor.submit(render_tile_for_zoom_level, zoom_level, tile, gdf)
            for tile in tiles
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Exception in process pool: {str(e)}")
                logging.error(traceback.format_exc())

def render_tiles():
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = [
            executor.submit(render_tiles_for_zoom_level, zoom_level)
            for zoom_level in range(MIN_ZOOM_LEVEL, MAX_ZOOM_LEVEL + 1)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Exception in process pool: {str(e)}")
                logging.error(traceback.format_exc())

render_tiles()
print("Done generating MBTiles.")
