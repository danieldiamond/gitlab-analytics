{% snapshot customers_db_customers_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='timestamp_with_deletes',
          updated_at='updated_at',
        )
    }}


    SELECT *, 0.1 AS eli_temp_col --TODO
    FROM {{ source('customers', 'customers_db_customers') }}
    WHERE _task_instance IN (SELECT MAX(_task_instance) FROM {{ source('customers', 'customers_db_customers') }}) --TODO: macro?
      AND id NOT IN (9983, 177735) --temp to test hard deletes
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

    
{% endsnapshot %}
