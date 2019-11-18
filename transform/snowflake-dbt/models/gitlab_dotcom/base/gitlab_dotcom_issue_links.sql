
{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'issue_links') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                      AS issue_link_id,
      source_id::INTEGER               AS source_id,
      target_id::INTEGER               AS target_id,
      created_at::TIMESTAMP            AS created_at,
      updated_at::TIMESTAMP            AS updated_at

    FROM source
    WHERE rank_in_key = 1
      AND created_at IS NOT NULL

)

SELECT *
FROM renamed
