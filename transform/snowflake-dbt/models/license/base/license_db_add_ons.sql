{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('license', 'license_db_add_ons') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER             AS add_on_id,
    name::VARCHAR           AS add_on_name,
    code::VARCHAR           AS add_on_code,
    created_at::TIMESTAMP   AS add_on_created_at,
    updated_at::TIMESTAMP   AS add_on_updated_at
 FROM source
 WHERE rank_in_key = 1

)

SELECT *
FROM renamed
