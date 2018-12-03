#!/bin/bash

cd $CI_PROJECT_DIR/transform/cloudsql-dbt

ci_helpers.py use_proxy "dbt docs generate --profiles-dir profile --target prod"

mkdir -p $CI_PROJECT_DIR/public/dbt/cloudsql
cd target
cp *.json *.html graph.gpickle $CI_PROJECT_DIR/public/dbt/cloudsql/

export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE^^}"
echo "SNOWFLAKE_DATABASE = $SNOWFLAKE_DATABASE"

cd $CI_PROJECT_DIR/transform/snowflake-dbt/
dbt deps --profiles-dir profile
dbt docs generate --profiles-dir profile --target prod
mkdir -p $CI_PROJECT_DIR/public/dbt/snowflake
cd target
cp *.json *.html graph.gpickle $CI_PROJECT_DIR/public/dbt/snowflake/
