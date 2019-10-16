{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'approvals') }}

), renamed AS (

    SELECT
      id::INTEGER                     AS approval_id,
      merge_request_id::INTEGER       AS merge_request_id,
      user_id::INTEGER                AS user_id,
      created_at::TIMESTAMP           AS approval_created_at,
      updated_at::TIMESTAMP           AS approval_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
