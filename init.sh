#!/bin/bash

# init script for airflow setup
mkdir -p ./dags ./logs ./plugins ./config ./database ./bucket # only needed if you didn't clone the dirs
sudo usermod -aG docker $USER
echo -e "AIRFLOW_UID=$(id -u)" > .env

#docker compose up airflow-init # initialize database, should exit with code 0

#docker compose down --volumes --remove-orphans # cleanup after running if you have big problems

#AWS issues
echo "fs.inotify.max_user_watches=1048576" >> /etc/sysctl.conf sysctl -p
sudo usermod -aG docker ssm-user

# s3 access read/write for bucket
s3fs ega-data-development s3bucket -o iam_role=XXXXX -o allow_other # put this into fstab!

# Install npm dependencies
echo "Installing npm dependencies..."
sudo apt-get install npm
npm init -y
npm install react react-dom leaflet
npx create-react-app .