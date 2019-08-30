{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'milestones') }}

), renamed AS (

    SELECT

      id :: integer                           AS milestone_id,
      project_id::integer                     AS project_id,
      group_id::integer                       AS group_id,
      start_date::date                        AS start_date,
      due_date::date                          AS due_date,
      state                                   AS milestone_status,

      created_at :: timestamp                 AS milestone_created_at,
      updated_at :: timestamp                 AS milestone_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
