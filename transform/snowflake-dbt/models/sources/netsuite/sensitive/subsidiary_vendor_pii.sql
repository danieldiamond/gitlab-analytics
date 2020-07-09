WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_vendor_source') }}

), entity_pii AS (

    SELECT
      entity_id,
      {{ nohash_sensitive_columns('netsuite_vendor_source', 'vendor_name') }}
    FROM source

)

SELECT *
FROM entity_pii
