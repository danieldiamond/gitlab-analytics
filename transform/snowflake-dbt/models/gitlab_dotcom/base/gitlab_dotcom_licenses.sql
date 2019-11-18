{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'licenses') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                                 AS license_id,
      created_at::TIMESTAMP                       AS created_at,
      updated_at::TIMESTAMP                       AS updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
