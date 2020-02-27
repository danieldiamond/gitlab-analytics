{% snapshot netsuite_departments_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key = 'department_id',
          check_cols = 'date_last_modified'
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'departments') }}

{% endsnapshot %}
