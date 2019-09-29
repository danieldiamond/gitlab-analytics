{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'milestones') }}

), renamed AS (

    SELECT

      id::INTEGER                           AS milestone_id,
      project_id::INTEGER                   AS project_id,
      group_id::INTEGER                     AS group_id,
      start_date::DATE                      AS start_date,
      due_date::DATE                        AS due_date,
      state::VARCHAR                        AS milestone_status,

      created_at::TIMESTAMP                 AS milestone_created_at,
      updated_at::TIMESTAMP                 AS milestone_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
