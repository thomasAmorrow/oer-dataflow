#!/bin/bash

# init script for oceexp-db setup

# Use PGDG repo for postgresql-16-h3 instead of sid
apt-get update 
apt-get install -y --no-install-recommends wget gnupg2 
apt-get install -y postgis postgresql-16-postgis-3 postgresql-16-postgis-3-scripts
wget --no-check-certificate -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - 
echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list 
apt-get update \
apt-get install -y --no-install-recommends postgresql-16-h3 
apt-get clean \
rm -rf /var/lib/apt/lists/*