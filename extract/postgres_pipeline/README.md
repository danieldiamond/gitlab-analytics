## Postgres_Pipeline (pgp) - Postgres Extractor

Data can be loaded from `postgres` into our data warehouse using `postgres_pipeline`.
All tables uploaded using `pgp` will contain a metadata column called `_uploaded_at` so that it can be determined when the data was loaded.

SCD (Slowly-Changing Dimensions):

Slowly-Changing dimensions are handled pretty simply. Each time the code is run, it will create a full copy of that table in the data warehouse.

* run `pgp` for SCD tables by invoking `python postgres_pipeline/main.py tap <manifest_path> --load_type scd`
* This command will tell `pgp` to only extract and load tables that are considered slowly-changing dimensions, it will skip all other tables
* A table is programmtically determined to be an SCD table if there is no `WHERE` clause in the raw query

Incremental:

* run `pgp` for Incremental tables by invoking `python postgres_pipeline/main.py tap <manifest_path> --load_type incremental`
* This command will tell `pgp` to only extract and load tables that are able to be incrementally loaded, it will skip all other tables
* A table is programmtically determined to be an incremental table if there is a `WHERE` clause in the raw query
* The time increment to load is based on the `execution_date` that is passed in by airflow minus the increment (`hours` or `days` depending on the query) passed in as an environment variable

Fully sync (backfilling):

* There are two conditions that would trigger a full backfill: 1) The table doesn't exist in snowflake or 2) The schema has changed (for instance a column was added or dropped or renamed even).
* `pgp` will look at the max ID of the target table and backfill in million ID increments, since, at GitLab, every table implemented is guaranteed to have an ID or some primary key

Validation (data quality check):

* _Documentation pending feature completion_

#### pgp manifest definition:

There are 5 mandatory sections and 1 optional sections in a `pgp` manifest.
The 5 sections are as follows:

1. `import_db`: the name of the database that is being imported from
1. `import_query`: this is the `SELECT` query that is used to extract data from the database. They usually target a single table
1. `export_schema`: this is the schema that the table lives in in the target database
1. `export_table`: this is the name of the table that is being targeted for export by the query
1. `export_table_primary_key`: this is the name of the column that is used as the primary key for the table. It is usually just `id`

The 6th optional section is called `additional_filtering`.
This field is used when you need to add an additional condition to the `import_query` that isn't related to incremental loading, for instance to filter some bad rows.


