{% snapshot netsuite_departments_snapshots %}

    {{
        config(
          strategy='check',
          unique_key = 'department_id',
          check_cols = 'all'
        )
    }}

    SELECT *
    FROM {{ source('netsuite', 'departments') }}

{% endsnapshot %}
