## Postgres_Pipeline (pgp) - Postgres Extractor

Data can be loaded from `postgres` into our data warehouse using `postgres_pipeline`.
All tables uploaded using `pgp` will contain a metadata column called `_uploaded_at` so that it can be determined when the data was loaded.

SCD (Slowly-Changing Dimensions):

Slowly-Changing dimensions are handled pretty simply. Each time the code is run, it will create a full copy of that table in the data warehouse in an append-only fashion.

* run `pgp` for SCD tables by invoking `python postgres_pipeline/main.py tap <manifest_path> --load_type scd`
* This command will tell `pgp` to only extract and load tables that are considered slowly-changing dimensions, it will skip all other tables
* A table is programmtically determined to be an SCD table if there is no `WHERE` clause in the raw query

Incremental (used by Airflow for time-specific loading and backfilling):

* run `pgp` for Incremental tables by invoking `python postgres_pipeline/main.py tap <manifest_path> --load_type incremental`
* This command will tell `pgp` to only extract and load tables that are able to be incrementally loaded, it will skip all other tables in addition to incremental tables that need to be fully re-synced
* A table is programmtically determined to be an incremental table if there is a `WHERE` clause in the raw query
* The time increment to load is based on the `execution_date` that is passed in by airflow minus the increment (`hours` or `days` depending on the query) passed in as an environment variable

Full sync (when a full-table backfill is required):

* There are two conditions that would trigger a full sync: 1) The table doesn't exist in snowflake (it is a new table) or 2) The schema has changed (for instance a column was added or dropped or renamed even).
* `pgp` will look at the max ID of the target table (the table in `Snowflake` the manifest describes) and backfill in chunks since. Note that this only works for tables that have some primary key. This is true for most tables at GitLab. We are not currently able to handle tables without a primary key.

Test:

* When a table has changed or is new (including SCD tables) `pgp` will try to load 1 million rows of that table to ensure that it can be loaded. This will catch the majority of data quality problems.

Validation (data quality check):

* _Documentation pending feature completion_

#### pgp manifest definition:

There are 5 mandatory sections and 2 optional sections in a `pgp` manifest.
The 5 sections are as follows:

1. `import_db`: the name of the database that is being imported from
1. `import_query`: this is the `SELECT` query that is used to extract data from the database. They usually target a single table
1. `export_schema`: this is the schema that the table lives in in the target database
1. `export_table`: this is the name of the table that is being targeted for export by the query
1. `export_table_primary_key`: this is the name of the column that is used as the primary key for the table. It is usually just `id`

The 6th optional section is called `additional_filtering`.
This field is used when you need to add an additional condition to the `import_query` that isn't related to incremental loading, for instance to filter some bad rows.

The 7th optional section is called `advanced_metadata`. This is a boolean field with the default being false and the only accepted value being `true`.
Adding this field requires a `drop` of the target table in `Snowflake` and a full re-sync. This field adds a `_task_instance` column to each upload so partitioning by Airflow runs is easier.

#### Technical Details:

The logical execution of data loading looks like the following:

1. The manifest is parsed and the table is processed
1. A check is done to see if the table exists or if the schema has changed
1. Depending on the above, the data is either loaded into a `_TEMP_` table or directly into the existing table
1. A query is run against the `postgres` DB, and a pointer is used to return chunks of the result-set
1. This data is then written out to a tab-separated file in a GCS bucket (the bucket is named `postgres_pipeline` in the `gitlab-analysis` project). Each table only has one file that it continually overwrites. The GCS bucket is set to purge files that are more than 30 days old.
1. A query is executed in Snowflake that triggers the loading of the file into the target table.
1. The next table is processed...

#### Tests

Tests are run in CI using `pytest`. `Snowflake` access and `postgres` access are both required, as they rely on the actual data sources for end-to-end testing.
The test file is located at `postgres_pipeline/test_postgres_pipeline.py`
