{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'labels') }}

),
renamed AS (

    SELECT

      id::INTEGER                                AS label_id,
      title                                        AS label_title,
      color,
      source.project_id::INTEGER                 AS project_id,
      group_id::INTEGER                          AS group_id,
      template,
      type                                         AS label_type,
      created_at::TIMESTAMP                      AS label_created_at,
      updated_at::TIMESTAMP                      AS label_updated_at

    FROM source
    WHERE rank_in_key = 1
)

SELECT *
FROM renamed
