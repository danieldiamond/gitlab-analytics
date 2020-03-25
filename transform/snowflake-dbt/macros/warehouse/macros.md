{% docs alter_warehouse %}
This macro turns on or off a Snowflake warehouse.
{% enddocs %}


{% docs backup_to_gcs %}
This macro fetches all relevant tables in the specified database and schema for backing up into GCS. This macro should NOT be used outside of a `dbt run-operation` command.
{% enddocs %}


{% docs get_backup_table_command %}
This macro is called by `backup_to_gcs` so that the actual `copy into` command can be generated. This macro should NOT be referenced outside of the `backup_to_gcs` macro.
{% enddocs %}


{% docs grant_usage_to_schemas %}
This macro...
{% enddocs %}


