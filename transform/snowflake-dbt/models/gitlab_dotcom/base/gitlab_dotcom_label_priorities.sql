{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'label_priorities') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::INTEGER                           AS label_priority_id,
      project_id::INTEGER                   AS project_id,
      label_id::INTEGER                     AS label_id,
      priority::INTEGER                     AS priority,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
