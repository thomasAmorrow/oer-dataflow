import csv
import json
import h3  # Correct import for h3-py v4.0+

input_csv = 'EGA_SCORE_HEX05.csv'  # Replace with your CSV file path
output_geojson = 'h3_hexagons_05.geojson'

features = []

# Function to adjust the longitude if it crosses the antimeridian
def adjust_longitudes_if_crosses_antimeridian(boundary):
    # Check if both positive and negative longitudes exist in the boundary
    longitudes = [lng for lat, lng in boundary]
    has_negative = any(lng < 0 for lng in longitudes)
    has_positive = any(lng > 0 for lng in longitudes)

    # If there are both negative and positive longitudes, check if the polygon crosses the antimeridian
    if has_negative and has_positive:
        # We adjust longitudes < 0 by adding 360 to bring them into the positive range
        boundary = [[lat, lng + 360 if lng < -90 else lng] for lat, lng in boundary]

    return boundary

# Open the CSV file
with open(input_csv, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        h3_index = row['hex_05']
        # Get the associated values
        combined_score = float(row['combined_score']) if row['combined_score'] else 0
        mapping_score = float(row['mapping_score']) if row['mapping_score'] else 0
        occurrence_score = float(row['occurrence_score']) if row['occurrence_score'] else 0
        chemistry_score = float(row['chemistry_score']) if row['chemistry_score'] else 0
        geology_score = float(row['geology_score']) if row['geology_score'] else 0


        # Get the boundary coordinates for the H3 cell
        geo = h3.cell_to_boundary(h3_index)
        centroid = h3.cell_to_latlng(h3_index)

        print(centroid)

        # Adjust the boundary coordinates if they cross the antimeridian (remembering lat/lng flip)
        geojson_boundary = adjust_longitudes_if_crosses_antimeridian(geo)

        # Convert to GeoJSON [lng, lat] order (flip back lat/lng)
        geojson_boundary = [[lng, lat] for lat, lng in geojson_boundary]
        centroid_geo = [centroid[1], centroid[0]]  # [lng, lat]
        print(centroid_geo)


        # Wrap this in a GeoJSON feature
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [geojson_boundary]
            },
            "properties": {
                "h3_index": h3_index,
                "combined": combined_score,
                "mapping": mapping_score,
                "chemistry": chemistry_score,
                "geology": geology_score,
                "occurrence": occurrence_score
            }
        }
        
        # Append the feature to the list
        features.append(feature)

# Create a GeoJSON FeatureCollection
geojson = {
    "type": "FeatureCollection",
    "features": features
}

# Write the GeoJSON data to a file
with open(output_geojson, 'w') as f:
    json.dump(geojson, f, indent=2)

print(f"GeoJSON saved to {output_geojson}")
