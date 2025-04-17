from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator
import h3
import time
import json

# Function to adjust the longitude if it crosses the antimeridian
def adjust_longitudes_if_crosses_antimeridian(boundary):
    longitudes = [lng for lat, lng in boundary]
    has_negative = any(lng < 0 for lng in longitudes)
    has_positive = any(lng > 0 for lng in longitudes)

    if has_negative and has_positive:
        boundary = [[lat, lng + 360 if lng < -90 else lng] for lat, lng in boundary]

    return boundary

# Function to fetch data from PostgreSQL
def fetch_data_from_pg(hexrez):
     # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT hex_05, combined_score, mapping_score, occurrence_score, chemistry_score, geology_score FROM ega_score_{hexrez}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


# Function to generate GeoJSON from database
def generate_geojson():

    timestr = time.strftime("%Y%m%d-%H%M%S")

    rows = fetch_data_from_pg('05')
    output_geojsonpoint = f'/mnt/s3bucket/h3_points_05_{timestr}.geojson'
    output_geojsonpoly = f'/mnt/s3bucket/h3_hexagons_05_{timestr}.geojson'
    featurespoly = []
    featurespoint = []

    for row in rows:
        h3_index, combined_score, mapping_score, occurrence_score, chemistry_score, geology_score = row

        # Get the boundary coordinates for the H3 cell
        geo = h3.cell_to_boundary(h3_index)
        centroid = h3.cell_to_latlng(h3_index)

        # Adjust the boundary coordinates if they cross the antimeridian (remembering lat/lng flip)
        geojson_boundary = adjust_longitudes_if_crosses_antimeridian(geo)

        # Convert to GeoJSON [lng, lat] order (flip back lat/lng)
        geojson_boundary = [[lng, lat] for lat, lng in geojson_boundary]
        centroid_geo = [centroid[1], centroid[0]]  # [lng, lat]

        # Wrap this in a GeoJSON feature
        featurepoly = {
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

                # Wrap this in a GeoJSON feature
        featurepoint = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [centroid_geo]
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
        featurespoly.append(featurepoly)
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

generate_geojson= PythonOperator(
    task_id='generate_geojson',
    python_callable=generate_geojson,
    provide_context=True,
    dag=dag
)

# Define task dependencies