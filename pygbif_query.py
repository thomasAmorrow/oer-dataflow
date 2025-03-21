import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
from shapely.geometry import Polygon, shape
from shapely.wkt import loads, dumps
from pygbif import occurrences as occ
import h3

# Function to check if a polygon's coordinates are counter-clockwise
def is_counter_clockwise(polygon_wkt):
    polygon = loads(polygon_wkt)
    return polygon.exterior.is_ccw

# Function to rearrange polygon to counter-clockwise
def rearrange_to_counter_clockwise(polygon_wkt):
    polygon = loads(polygon_wkt)
    if not polygon.exterior.is_ccw:
        polygon = Polygon(list(polygon.exterior.coords)[::-1])
    return dumps(polygon)

# gimme a cell near Hawaii
cells = ['825d1ffffffffff']

# gimme the polys
polygon = h3.cells_to_geo(cells, tight=True)

# Print polygon geometry
print("Polygon geometry:", polygon)

polygeo = shape(polygon)

# Check if the polygon is counter-clockwise
print("Is the polygon counter-clockwise?", polygeo.exterior.is_ccw)

# If it's not counter-clockwise, rearrange it
if not polygeo.exterior.is_ccw:
    print("Rearranging polygon to counter-clockwise...")
    polygon_wkt = dumps(polygeo)
    rearranged_wkt = rearrange_to_counter_clockwise(polygon_wkt)
    polygeo = shape(loads(rearranged_wkt))

# Search for critters (occurrences) within the polygon
critters = occ.search(geometry=polygeo.wkt, limit=10)
#print("Critters found:", critters)

# Extract latitude and longitude from the first critter
latitude = critters['results'][0]['decimalLatitude']
longitude = critters['results'][0]['decimalLongitude']
print(f"Critter Location: Latitude = {latitude}, Longitude = {longitude}")

# Plot the polygon on a map using GeoPandas and Matplotlib
gdf = gpd.GeoDataFrame([polygeo], columns=['geometry'])
gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)

# Create a plot
fig, ax = plt.subplots(figsize=(8, 8))

# Plot the polygon
gdf.plot(ax=ax, color='lightblue', edgecolor='black', alpha=0.7)

# Add a basemap using contextily
# Reproject to web mercator (EPSG:3857) for compatibility with contextily basemaps
gdf = gdf.to_crs(epsg=4326)

# Add OpenStreetMap basemap
ctx.add_basemap(ax, crs=gdf.crs)

# Plot the critter location as a dot
ax.scatter(longitude, latitude, color='red', s=100, label='Critter Location')

# Set plot title
ax.set_title("Polygon Location Map with Critter", fontsize=15)

# Optionally, adjust the extent of the plot to focus on the polygon
#ax.set_xlim([-160,-150])  # xmin, xmax
#ax.set_ylim([15, 25])  # ymin, ymax

# Show the plot
plt.legend()
plt.show()
