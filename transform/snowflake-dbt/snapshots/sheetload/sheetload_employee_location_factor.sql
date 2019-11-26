{% snapshot sheetload_employee_location_factor_snapshots %}

    {{
        config(
          target_database=env_var('SNOWFLAKE_LOAD_DATABASE'),
          target_schema='snapshots',
          unique_key='"Employee_ID"',
          strategy='timestamp',
          updated_at='_UPDATED_AT',
        )
    }}

    SELECT *
    FROM {{ source('sheetload', 'employee_location_factor') }}
    WHERE "Employee_ID" != ''
    AND "Location_Factor" NOT LIKE '#N/A' ESCAPE '#'

{% endsnapshot %}
