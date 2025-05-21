from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import h3
import json
import time
import csv

# Function to check if a polygon crosses the antimeridian
def crosses_antimeridian(boundary):
    """Check if the polygon crosses the antimeridian."""
    longitudes = [lng for lat, lng in boundary]
    return any(lng <= -90 and longitudes[i+1] > 90 or lng >= 90 and longitudes[i+1] < -90
               for i, lng in enumerate(longitudes[:-1]))

# Function to adjust the boundary if it crosses the antimeridian
# this is really ugly for now but need to see if it works for ArcGIS exports
def adjust_longitudes_if_crosses_antimeridian(boundary):
    first_half = []
    #second_half = []
    #intersection_points = []

    for i, (lat, lng) in enumerate(boundary):

        if lng < 0:
            first_half.append([lat, lng + 360])
        else:
            first_half.append([lat, lng])
        
        #next_lat, next_lng = boundary[(i + 1) % len(boundary)]  # next point, cyclically

        # Handle edges crossing the meridian
        #if (lng <= -180 and next_lng >= 180) or (lng >= 180 and next_lng <= -180):
        #    intersection_point = [lat, 180 if lng < 0 else -180]
        #    intersection_points.append(intersection_point)

        #if lng < 0:
        #    first_half.append([lat, lng])
        #else:
        #    second_half.append([lat, lng])

    # Insert intersection points if they exist
    #if intersection_points:
    #    first_half.append(intersection_points[0])
    #    second_half.insert(0, intersection_points[1])

    #return first_half, second_half
    return first_half

# Function to check if a polygon is closed
def close_polygon(polygon):
    """Ensure that the polygon is closed by checking if the last point matches the first."""
    if not polygon:
        raise ValueError("Polygon is empty and cannot be closed.")  # Raise error if empty
    
    if polygon[0] != polygon[-1]:
        polygon.append(polygon[0])  # Close the polygon by repeating the first point at the end
    
    return polygon

# Function to fetch data from PostgreSQL
def fetch_data_from_pg(hexrez):
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT hex_{hexrez}, COALESCE(combined_score,0), COALESCE(mapping_score,0), COALESCE(occurrence_score,0), COALESCE(chemistry_score,0), COALESCE(geology_score,0), COALESCE(edna_score,0), COALESCE(wcsd_score,0) FROM ega_score_{hexrez}")
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
            h3_index, combined_score, mapping_score, occurrence_score, chemistry_score, geology_score, edna_score, wcsd_score = row

            # Get the boundary coordinates for the H3 cell
            geo = h3.cell_to_boundary(h3_index)
            centroid = h3.cell_to_latlng(h3_index)

            # Check if the polygon crosses the antimeridian
            if crosses_antimeridian(geo):
                geojson_boundary2 = []
                # Adjust the boundary coordinates if they cross the antimeridian
                #geojson_boundary1, geojson_boundary2 = adjust_longitudes_if_crosses_antimeridian(geo)
                geojson_boundary1 = adjust_longitudes_if_crosses_antimeridian(geo)

                # Convert to GeoJSON [lng, lat] order (flip back lat/lng)
                geojson_boundary1 = [[lng, lat] for lat, lng in geojson_boundary1]
                if geojson_boundary2:
                    geojson_boundary2 = [[lng, lat] for lat, lng in geojson_boundary2]

                # Ensure the polygons are closed
                try:
                    geojson_boundary1 = close_polygon(geojson_boundary1)
                except ValueError as e:
                    print(f"Skipping invalid polygon (boundary1): {e}")
                    continue

                if geojson_boundary2:
                    try:
                        geojson_boundary2 = close_polygon(geojson_boundary2)
                    except ValueError as e:
                        print(f"Skipping invalid polygon (boundary2): {e}")
                        continue

                # Create two separate GeoJSON features for the two halves of the split polygon

                # First half polygon (negative side)
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
                        "occurrence": occurrence_score,
                        "edna_score": edna_score,
                        "wcsd_score": wcsd_score
                    }
                }

                # Second half polygon (positive side)
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
                            "occurrence": occurrence_score,
                            "edna_score": edna_score,
                            "wcsd_score": wcsd_score
                        }
                    }

                # Add both polygons to the list
                featurespoly.append(featurepoly1)
                if geojson_boundary2:
                    featurespoly.append(featurepoly2)
            else:
                # If the polygon doesn't cross the antimeridian, process it normally
                geojson_boundary = [[lng, lat] for lat, lng in geo]
                geojson_boundary = close_polygon(geojson_boundary)

                # Create a single GeoJSON feature for the polygon
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
                        "occurrence": occurrence_score,
                        "edna_score": edna_score,
                        "wcsd_score": wcsd_score
                    }
                }

                # Add the polygon to the list
                featurespoly.append(featurepoly)

            # Wrap the centroid in a GeoJSON point feature
            centroid_geo = [centroid[1], centroid[0]]  # [lng, lat]
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
                    "occurrence": occurrence_score,
                    "edna_score": edna_score,
                    "wcsd_score": wcsd_score
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

def generate_csv():
    import csv
    for rez in ['03', '04', '05']:
        rows = fetch_data_from_pg(rez)
        output_csv = f'/mnt/s3bucket/h3_scores_{rez}.csv'

        with open(output_csv, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "h3_index", "combined", "mapping", "occurrence",
                "chemistry", "geology", "edna_score", "wcsd_score"
            ])
            for row in rows:
                writer.writerow(row)

        print(f"CSV saved to {output_csv}")

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
    'output_scores',
    default_args=default_args,
    description='DAG to output geojson and csv score files',
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

generate_csv = PythonOperator(
    task_id='generate_csv',
    python_callable=generate_geojson,
    provide_context=True,
    dag=dag
)

# Define task dependencies
generate_geojson
generate_csv
