{% docs case_when_boolean_int %}
This macro returns a 1 if some value is greater than 0; otherwise, it returns a 0.
{% enddocs %}


{% docs coalesce_to_infinity %}
This macro expects a timestamp or date column as an input. If a non-null value is inputted, the same value is returned. If a null value is inputted, a large date representing 'infinity' is returned. This is useful for writing `BETWEEN` clauses using date columns that are sometimes NULL.
{% enddocs %}


{% docs create_snapshot_base %}
This macro creates a base model for dbt snapshots. A single entry is generated from the chosen start date through the current date for the specified primary key(s) and unit of time.
{% enddocs %}


{% docs current_date_schema %}
Returns the schema name based on the run start time. Returns `base_yyyy_mm`.
{% enddocs %}


{% docs distinct_source %}
This macro is used for condensing a `source` CTE into unique rows only. Our ETL runs quite frequently while most rows in our source tables don't update as frequently. So we end up with a lot of rows in our RAW tables that look the same as each other (except for the metadata columns with a leading underscore). This macro takes in a `source_cte` and looks for unique values across ALL columns (excluding airflow metadata.)

This macro **is specific** to pgp tables (gitlab_dotcom, version, license) and should not be used outside of those. Specifically, it makes references to 2 airflow metadata columns:
* `_uploaded_at`: we only want the *minimum* value per unique row ... AKA "when did we *first* see this unique row?" This macros calls this column `valid_from` (to be used in the SCD Type 2 Macro)
* `_task_instance`: we want to know the *maximum* task instance (what was the last task when we saw this row?). This is used later to infer whether a `primary_key` is still present in the source table (as a roundabout way to track hard deletes)

{% enddocs %}


{% docs generate_schema_name %}
This is the GitLab overwrite for the dbt internal macro. See our [dbt guide](https://about.gitlab.com/handbook/business-ops/data-team/dbt-guide/#general) for more info on how this works.
{% enddocs %}


{% docs monthly_change %}
This macro calculates differences for each consecutive usage ping by uuid.
{% enddocs %}


{% docs monthly_is_used %}
This macro includes the total counts for a given feature's usage cumulatively.
{% enddocs %}


{% docs query_comment %}
Defines the format for how comments are added to queries. See [dbt documentation](https://docs.getdbt.com/docs/building-a-dbt-project/dbt-projects/configuring-query-comments/).
{% enddocs %}


{% docs scd_type_2 %}
This macro inserts SQL statements that turn the inputted CTE into a [type 2 slowly changing dimension model](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row). According to [Orcale](https://www.oracle.com/webfolder/technetwork/tutorials/obe/db/10g/r2/owb/owb10gr2_gs/owb/lesson3/slowlychangingdimensions.htm), "a Type 2 SCD retains the full history of values. When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active."

In particular, this macro adds 3 columns: `valid_from`, `valid_to`, and `is_currently_valid`. It does not alter or drop any of the existing columns in the input CTE.
* `valid_from` will never be null
* `valid_to` can be NULL for up to one row per ID. It is possible for an ID to have 0 currently active rows (implies a "Hard Delete" on the source db)
* `is_currently_active` will be TRUE in cases where `valid_to` is NULL (for either 0 or 1 rows per ID)

The parameters are as follows:
  * **primary_key_renamed**: The primary key column from the `casted_cte`. According to our style guide, we usually rename primary keys to include the table name ("merge_request_id")
  * **primary_key_raw**: The same column as above, but references the column name from when it was in the RAW schema (usually "id")
  * **source_cte**: (defaults to '`distinct_source`). This is the name of the CTE with all of the unique rows from the raw source table. This will always be `distinct_source` if using the `distinct_source` macro.
  * **casted_cte**: (defaults to `renamed`). This is the name of the CTE where all of the columns have been casted and renamed. Our internal convention is to call this `renamed`. This CTE needs to have a column called `valid_from`.

This macro does **not** reference anything specific to the pgp data sources, but was built with them in mind. It is unlikely that this macro will be useful to anything outside of pgp data sources as it was built for a fairly specific problem. We would have just used dbt snapshots here except for the fact that they currently don't support hard deletes. dbt snapshots should be satisfactory for most other use cases.

This macro was built to be used in conjunction with the distinct_source macro.

{% enddocs %}


{% docs schema_union_all %}
This macro takes a schema prefix and a table name and does a UNION ALL on all tables that match the pattern. The exclude_part parameter defaults to 'scratch' and all schemas matching that pattern will be ignored.
{% enddocs %}


{% docs schema_union_limit %}
This macro takes a schema prefix, a table name, a column name, and an integer representing days. It returns a view that is limited to the last 30 days based on the column name. Note that this also calls schema union all which can be a heavy call.
{% enddocs %}


