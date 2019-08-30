{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'issue_links') }}

), renamed AS (

    SELECT
      id :: integer                      AS issue_link_id,
      source_id :: integer               AS source_id,
      target_id :: integer               AS target_id,
      created_at :: timestamp            AS issue_link_created_at,
      updated_at :: timestamp            AS issue_link_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
