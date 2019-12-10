{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'merge_requests_closing_issues') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT DISTINCT 
      id::INTEGER                AS merge_request_issue_relation_id,
      merge_request_id::INTEGER  AS merge_request_id,
      issue_id::INTEGER          AS issue_id,
      created_at::TIMESTAMP      AS created_at,
      updated_at::TIMESTAMP      AS updated_at

    FROM source

)

SELECT *
FROM renamed
