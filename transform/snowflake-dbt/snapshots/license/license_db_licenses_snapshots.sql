{% snapshot license_db_licenses_snapshots %}

    {{
        config(
          target_database='RAW',
          target_schema='snapshots',
          unique_key='id',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}
    
    WITH source AS (

      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS license_rank_in_key
      FROM {{ source('license', 'licenses') }}

    )

    SELECT *
    FROM source
    WHERE license_rank_in_key = 1
    
{% endsnapshot %}
