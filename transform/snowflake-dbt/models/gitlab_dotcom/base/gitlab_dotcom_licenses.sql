{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'licenses') }}

), renamed AS (

    SELECT

      id::INTEGER                                 AS license_id,
      created_at::TIMESTAMP                       AS license_created_at,
      updated_at::TIMESTAMP                       AS license_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed