{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'boards') }}

), renamed AS (

    SELECT
      id :: integer              AS board_id,
      project_id :: integer      AS project_id,
      created_at :: timestamp    AS board_created_at,
      updated_at :: timestamp    AS board_updated_at,
      milestone_id :: integer    AS milestone_id,
      group_id :: integer        AS group_id,
      weight :: integer          AS weight

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
