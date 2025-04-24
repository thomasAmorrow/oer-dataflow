import json
from pathlib import Path

def ensure_closed_rings(input_path, output_path):
    with open(input_path, 'r') as f:
        data = json.load(f)

    for feature in data['features']:
        geom_type = feature['geometry']['type']
        if geom_type == 'Polygon':
            coords = feature['geometry']['coordinates'][0]
            if coords[0] != coords[-1]:
                coords.append(coords[0])  # Close the ring
                feature['geometry']['coordinates'][0] = coords

    with open(output_path, 'w') as f:
        json.dump(data, f)

# Loop over all zoom levels
input_dir = Path("app/geo")
output_dir = Path("app/geo")
output_dir.mkdir(parents=True, exist_ok=True)

zoom_levels = ['03', '04', '05']

for zoom in zoom_levels:
    input_file = input_dir / f"h3_hexagons_{zoom}.geojson"
    output_file = output_dir / f"h3_hexagons_{zoom}_closed.geojson"
    ensure_closed_rings(input_file, output_file)
    print(f"Wrote closed version: {output_file}")
