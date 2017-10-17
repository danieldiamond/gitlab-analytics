#!/bin/bash

# Exit on Err
set -e

# import config file
source /opt/bizops/config/bizops.conf

# Start postgres

echo "Starting Postgres..."

sudo -u postgres -H sh -c "/usr/lib/postgresql/10/bin/postgres -D /var/lib/postgresql/10/main -c config_file=/etc/postgresql/10/main/postgresql.conf >/var/log/postgresql/logfile 2>&1 &"

echo "Postgres started. Superset Admin User being created..."

# Create admin user for superset
fabmanager create-admin --username $ss_admin_user --firstname $ss_admin_first --lastname $ss_admin_last --email $ss_admin_email --password $ss_admin_pass --app superset

echo "Superset Admin user created"

# Update Superset db after creating admin user
superset db upgrade

echo "Superset database updated. Loading examples. This could take a couple of minutes..."

# Load Superset example data/reports - Optional and not small or quick.
superset load_examples

echo "Loaded Superset Examples. Starting Superset Server..."

# Initialize Superset
superset init 

# Start Superst
superset runserver
