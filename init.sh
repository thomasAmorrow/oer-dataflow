#!/bin/bash

# init script for airflow setup
mkdir -p ./dags ./logs ./plugins ./config ./database ./bucket ./s3bucket # only needed if you didn't clone the dirs
sudo usermod -aG docker $USER
echo -e "AIRFLOW_UID=$(id -u)" > .env

#docker compose up airflow-init # initialize database, should exit with code 0