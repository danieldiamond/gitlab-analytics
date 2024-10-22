{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'milestones') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                           AS milestone_id,
      title::VARCHAR                        AS milestone_title,
      description::VARCHAR                  AS milestone_description,
      project_id::NUMBER                   AS project_id,
      group_id::NUMBER                     AS group_id,
      start_date::DATE                      AS start_date,
      due_date::DATE                        AS due_date,
      state::VARCHAR                        AS milestone_status,

      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source

)

SELECT *
FROM renamed
