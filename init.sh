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

sudo echo "user_allow_other" >> /etc/fuse.conf
mount-s3 ega-data-XXXXXX /oer-ega/bucket --allow-other



# Install npm dependencies
echo "Installing npm dependencies..."
sudo apt-get install npm
npm init -y
npm install react react-dom leaflet
npx create-react-app .