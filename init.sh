#!/bin/bash

# init script for airflow setup
mkdir -p ./dags ./logs ./plugins ./config # only needed if you didn't clone the dirs
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init # initialize database, should exit with code 0

#docker compose down --volumes --remove-orphans # cleanup after running if you have big problems