{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('license', 'granted_add_ons') }}

), renamed AS (

  SELECT DISTINCT
    id::INTEGER            AS granted_add_on_id,
    license_id::INTEGER    AS license_id,
    add_on_id::INTEGER     AS add_on_id,
    quantity::INTEGER      AS quantity,
    created_at::TIMESTAMP  AS created_at,
    updated_at::TIMESTAMP  AS updated_at
 FROM source
 WHERE rank_in_key = 1

)

SELECT *
FROM renamed
