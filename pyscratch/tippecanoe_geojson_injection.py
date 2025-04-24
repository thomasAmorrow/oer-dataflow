import json
from pathlib import Path

# List of (input_path, output_path, minzoom, maxzoom)
files = [
    ("app/geo/h3_hexagons_03_closed.geojson", "app/geo/h3_hexagons_03_zfiltered.geojson", 3, 6),
    ("app/geo/h3_hexagons_04_closed.geojson", "app/geo/h3_hexagons_04_zfiltered.geojson", 7, 9),
    ("app/geo/h3_hexagons_05_closed.geojson", "app/geo/h3_hexagons_05_zfiltered.geojson", 10, 12),
]

for input_path, output_path, minzoom, maxzoom in files:
    print(f"Processing {input_path} → {output_path}")
    
    with open(input_path, "r", encoding="utf-8") as infile:
        geojson = json.load(infile)

    for feature in geojson.get("features", []):
        feature["tippecanoe"] = {
            "minzoom": minzoom,
            "maxzoom": maxzoom
        }

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump(geojson, outfile, indent=2)

print("✅ Done adding zoom metadata.")
