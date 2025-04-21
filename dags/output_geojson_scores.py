from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import h3
import time
import json

# Function to adjust the boundary if it crosses the antimeridian
def adjust_longitudes_if_crosses_antimeridian(boundary):
    # First, we'll split the boundary into two parts: one on the -180 side and one on the +180 side
    first_half = []
    second_half = []
    intersection_points = []

    for i, (lat, lng) in enumerate(boundary):
        # Check if we need to split at the meridian
        next_lat, next_lng = boundary[(i + 1) % len(boundary)]  # next point, cyclically

        # Handle edges crossing the meridian
        if (lng <= -180 and next_lng >= 180) or (lng >= 180 and next_lng <= -180):
            # Compute intersection points at the antimeridian (longitude = 180 or -180)
            intersection_point = [lat, 180 if lng < 0 else -180]
            intersection_points.append(intersection_point)

        if lng < 0:
            first_half.append([lat, lng])
        else:
            second_half.append([lat, lng])

    # Insert intersection points where appropriate to ensure the split is valid
    if intersection_points:
        first_half.append(intersection_points[0])
        second_half.insert(0, intersection_points[1])

    return first_half, second_half


# Function to check if a polygon is closed
def close_polygon(polygon):
    """Ensure that the polygon is closed by checking if the last point matches the first."""
    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])  # Close the polygon by repeating the first point at the end
    return polygon


# Function to fetch data from PostgreSQL
def fetch_data_from_pg(hexrez):
     # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT hex_{hexrez}, combined_score, mapping_score, occurrence_score, chemistry_score, geology_score FROM ega_score_{hexrez}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


# Function to generate GeoJSON from database
def generate_geojson():
    timestr = time.strftime("%Y%m%d-%H%M%S")

    for rez in ['03', '04', '05']:
        rows = fetch_data_from_pg(rez)
        output_geojsonpoint = f'/mnt/s3bucket/h3_points_{rez}.geojson'
        output_geojsonpoly = f'/mnt/s3bucket/h3_hexagons_{rez}.geojson'
        featurespoly = []
        featurespoint = []

        for row in rows:
            h3_index, combined_score, mapping_score, occurrence_score, chemistry_score, geology_score = row

            # Get the boundary coordinates for the H3 cell
            geo = h3.cell_to_boundary(h3_index)
            centroid = h3.cell_to_latlng(h3_index)

            # Adjust the boundary coordinates if they cross the antimeridian
            geojson_boundary1, geojson_boundary2 = adjust_longitudes_if_crosses_antimeridian(geo)

            # Convert to GeoJSON [lng, lat] order (flip back lat/lng)
            geojson_boundary1 = [[lng, lat] for lat, lng in geojson_boundary1]
            geojson_boundary2 = [[lng, lat] for lat, lng in geojson_boundary2]

            # Ensure the polygons are closed
            geojson_boundary1 = close_polygon(geojson_boundary1)
            geojson_boundary2 = close_polygon(geojson_boundary2)

            centroid_geo = [centroid[1], centroid[0]]  # [lng, lat]

            # Wrap the first half in a GeoJSON feature
            featurepoly1 = {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [geojson_boundary1]
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
            
            # Wrap the second half in a GeoJSON feature if it exists
            featurepoly2 = {}
            if geojson_boundary2:
                featurepoly2 = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [geojson_boundary2]
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

            # Add the polygon features to the list
            featurespoly.append(featurepoly1)
            if geojson_boundary2:
                featurespoly.append(featurepoly2)

            # Wrap the centroid in a GeoJSON point feature
            featurepoint = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": centroid_geo
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

            # Append the point feature to the list
            featurespoint.append(featurepoint)

        # Create a GeoJSON FeatureCollection for the polygons
        geojsonpoly = {
            "type": "FeatureCollection",
            "features": featurespoly
        }

        # Create a GeoJSON FeatureCollection for the points
        geojsonpoint = {
            "type": "FeatureCollection",
            "features": featurespoint
        }

        # Write the GeoJSON poly data to a file
        with open(output_geojsonpoly, 'w') as f:
            json.dump(geojsonpoly, f, indent=2)

        print(f"GeoJSON saved to {output_geojsonpoly}")

        # Write the GeoJSON point data to a file
        with open(output_geojsonpoint, 'w') as f:
            json.dump(geojsonpoint, f, indent=2)

        print(f"GeoJSON saved to {output_geojsonpoint}")


# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'output_geojson_scores',
    default_args=default_args,
    description='DAG to output geojson score files for webmap display',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

generate_geojson = PythonOperator(
    task_id='generate_geojson',
    python_callable=generate_geojson,
    provide_context=True,
    dag=dag
)

# Define task dependencies
