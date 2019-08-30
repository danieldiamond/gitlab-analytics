{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'approvals') }}

), renamed AS (

    SELECT
      id :: integer                     AS approval_id,
      merge_request_id :: integer       AS merge_request_id,
      user_id :: integer                AS user_id,
      created_at :: timestamp           AS approval_created_at,
      updated_at :: timestamp           AS approval_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
