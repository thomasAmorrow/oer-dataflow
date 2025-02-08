FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y \
    postgis \
    postgresql-client \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

USER airflow