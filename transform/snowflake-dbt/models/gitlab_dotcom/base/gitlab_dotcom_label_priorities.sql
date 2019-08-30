{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'label_priorities') }}

), renamed AS (

    SELECT

      id :: integer                           AS label_priority_id,
      project_id :: integer                   AS project_id,
      label_id :: integer                     AS label_id,
      priority :: integer                     AS priority,
      created_at :: timestamp                 AS label_priority_created_at,
      updated_at :: timestamp                 AS label_priority_updated_at


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
