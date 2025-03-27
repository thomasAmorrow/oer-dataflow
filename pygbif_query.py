import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
import shapely
from shapely.geometry import Polygon, shape, LineString
from shapely.wkt import loads, dumps
from pygbif import occurrences as occ
import h3
import pandas as pd
from shapely.ops import split
import antimeridian

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
cells = ['815d7ffffffffff']
cells2 = ['835da5fffffffff']

# gimme the polys
polygon = h3.cells_to_geo(cells, tight=True)
polygon2 = h3.cells_to_geo(cells2, tight=True)


# Print polygon geometry
print("Polygon geometry:", polygon)

polygeo = shape(polygon)
polygeo2 = shape(polygon2)

# Check if the polygon is counter-clockwise
print("Is the polygon counter-clockwise?", polygeo.exterior.is_ccw)

# If it's not counter-clockwise, rearrange it
#if not polygeo.exterior.is_ccw:
#    print("Rearranging polygon to counter-clockwise...")
#    polygon_wkt = dumps(polygeo)
#    rearranged_wkt = rearrange_to_counter_clockwise(polygon_wkt)
#    polygeo = shape(loads(rearranged_wkt))

def crossing_antimeridian(hexagon):

    minx, miny, maxx, maxy = hexagon.bounds
    if abs(minx-maxx) > 180:
        print('Hexagon crosses antimeridian, fixing...')
        hexagon = antimeridian.fix_polygon(hexagon)

    return hexagon
    
polygeo = crossing_antimeridian(polygeo)

# Search for critters (occurrences) within the polygon
critters = occ.search(geometry=polygeo.wkt, limit=20000, depth="200,10000", fields=['latitude','longitude','depth','taxonKey','scientificName', 'kingdomKey', 'phylumKey', 'classKey', 'orderKey', 'familyKey', 'genusKey', 'basisOfRecord'])
minx, miny, maxx, maxy = polygeo.bounds
print(f"Coords are {minx}, {miny}, {maxx}, {maxy}")
#query = (['depth > 200', 'decimalLatitude > + miny', 'decimalLatitude < {maxy}', 'decimalLongitude > {minx}', 'decimalLongitude < {maxx}'], pred_type ='and')
#print(query)
polygon_wkt = dumps(polygeo)
polygon2_wkt = dumps(polygeo2)
#query = 'geometry = {polygon_wkt}', 'limit = 100000', 'depth = 200,12000'  # Specify depth range correctly (check API documentation)
occdatakey, occdatastring=occ.download(
    format='SIMPLE_CSV',
    user="oerdevops",
    pwd="oceanexploration",
    email="oar.oer.devops@noaa.gov",
    queries= ['depth > 200', 'hasGeospatialIssue = FALSE', 'hasCoordinate = TRUE']
)
print(occdatakey)
occ.download_get(key=occdatakey,path=str(cells[0]))

#print("Critters found:", critters)

# Prepare a list to store occurrences
occurrences = []

# Extract latitude, longitude, depth, and taxonomy information from the critters
for critter in critters['results']:
    latitude = critter['decimalLatitude']
    longitude = critter['decimalLongitude']
    depth = critter['depth']  # 'None' if depth is not available
    taxonkey = critter['taxonKey']
    scientificname = critter['scientificName']
    kingdom = critter.get('kingdomKey', None)
    phylum = critter.get('phylumKey', None)
    class_key = critter.get('classKey', None)
    order = critter.get('orderKey', None)
    family = critter.get('familyKey', None)
    genus = critter.get('genusKey', None)
    basisofrecord = critter.get('basisOfRecord', None)
    #subgenus = critter.get('subgenusKey', None)
    
    # Only add the critter to occurrences if depth is available
    if depth is not None:
        occurrences.append({
            'latitude': latitude,
            'longitude': longitude,
            'depth': depth,
            'taxonkey' : taxonkey,
            'scientificname' : scientificname,
            'kingdomKey': kingdom,
            'phylumKey': phylum,
            'classKey': class_key,
            'orderKey': order,
            'familyKey': family,
            'genusKey': genus,
            'basisofrecord' : basisofrecord
            #'subgenusKey': subgenus
        })

# Convert occurrences list to DataFrame and print only those with depth
#occurrences_df = pd.DataFrame(occurrences)
#print(occurrences_df)
#occurrences_df.to_csv('occurrences.csv', index=False)

# Plot the polygon on a map using GeoPandas and Matplotlib
#gdf = gpd.GeoDataFrame([polygeo], columns=['geometry'])
#gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)

# Create a plot
#fig, ax = plt.subplots(figsize=(8, 8))

# Plot the polygon
#gdf.plot(ax=ax, color='lightblue', edgecolor='black', alpha=0.7)

# Reproject to web mercator (EPSG:3857) for compatibility with contextily basemaps
#gdf = gdf.to_crs(epsg=3857)

# Add OpenStreetMap basemap
#ctx.add_basemap(ax, crs=gdf.crs)

# Plot the critter locations as red dots
#for critter in occurrences:
#    ax.scatter(critter['longitude'], critter['latitude'], color='red', s=100, label='Critter Location')

# Set plot title
#ax.set_title("Polygon Location Map with Critter", fontsize=15)

# Show the plot
#plt.legend()
#plt.show()


# JUST SAVE THIS FOR LATER
# awk -F'\t' 'NF == 50 {for(i=1; i<=NF; i++) $i="\"" $i "\""; print $0}' OFS='\t' 0002632-250325103851331.csv > cleaned_NR50.csv


# AND THIS
#COPY gbif_occurrences (
#    gbifid,
#    datasetkey,
#    occurrenceid,
#    kingdom,
#    phylum,
#    "class",
#    "order",
#    family,
#   genus,
#    species,
#    infraspecificepithet,
#    taxonrank,
#    scientificname,
#    verbatimscientificname,
#    verbatimscientificnameauthorsh,
#    countrycode,
#    locality,
#    stateprovince,
#    occurrencestatus,
#    individualcount,
#    publishingorgkey,
#    decimallatitude,
#    decimallongitude,
#    coordinateuncertaintyinmeters,
#    coordinateprecision,
#    elevation,
#    elevationaccuracy,
#    depth,
#    depthaccuracy,
#    eventdate,
#    day,
#    month,
#    year,
#    taxonkey,
#    specieskey,
#    basisofrecord,
#    institutioncode,
#    collectioncode,
#    catalognumber,
#    recordnumber,
#    identifiedby,
#    dateidentified,
#    license,
#    rightsholder,
#    recordedby,
#    typestatus,
#    establishmentmeans,
#    lastinterpreted,
#    mediatype,
#    issue
#)
#FROM '/var/lib/postgresql/data/cleaned_NR50'
#WITH (FORMAT csv, HEADER true, DELIMITER E'\t', QUOTE '"');

#ALTER TABLE gbif_occurrences ADD COLUMN location GEOMETRY(point, 4326);
#UPDATE gbif_occurrences SET location = ST_SETSRID(ST_MakePoint(cast(decimallongitude as float), cast(decimallatitude as float)),4326)