WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_subsidiary_source') }}

), entity_pii AS (

    SELECT
      entity_id,
      {{ nohash_sensitive_columns('netsuite_subsidiary_source', 'subsidiary_name') }}
    FROM source

)

SELECT *
FROM entity_pii
