{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'label_priorities') }}

), renamed AS (

    SELECT

      id::INTEGER                           AS label_priority_id,
      project_id::INTEGER                   AS project_id,
      label_id::INTEGER                     AS label_id,
      priority::INTEGER                     AS priority,
      created_at::TIMESTAMP                 AS label_priority_created_at,
      updated_at::TIMESTAMP                 AS label_priority_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
