#!/bin/bash
tap=".meltano/extractors/tap-postgres/venv/bin/tap-postgres"
tap_config="-c extract/postgres/config/tap_postgres/$EXPORT_DATABASE/config.json"
tap_catalog="-p extract/postgres/config/tap_postgres/$EXPORT_DATABASE/catalog.json"
tap_state="-s extract/postgres/config/tap_postgres/$EXPORT_DATABASE/state.json"

target=".meltano/loaders/target-snowflake/venv/bin/target-snowflake"
target_config="-c extract/postgres/config/target_snowflake/config.json"

ci_helpers.py use_proxy "$tap $tap_config $tap_catalog $tap_state | $target $target_config"