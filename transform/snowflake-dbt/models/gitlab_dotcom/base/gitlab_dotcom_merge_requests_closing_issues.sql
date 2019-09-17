{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY merge_request_id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_requests_closing_issues') }}

), renamed AS (

    SELECT
      DISTINCT md5(merge_request_id || issue_id || created_at)    as merge_request_issue_relation_id,
      merge_request_id :: integer                                 as merge_request_id,
      issue_id :: integer                                         as issue_id,
      created_at :: timestamp                                     as merge_request_closing_issue_created_at,
      updated_at :: timestamp                                     as merge_request_closing_issue_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
