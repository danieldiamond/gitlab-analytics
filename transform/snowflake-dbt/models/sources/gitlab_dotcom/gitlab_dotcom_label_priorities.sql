WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'label_priorities') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                           AS label_priority_id,
      project_id::NUMBER                   AS project_id,
      label_id::NUMBER                     AS label_id,
      priority::NUMBER                     AS priority,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source

)

SELECT *
FROM renamed
