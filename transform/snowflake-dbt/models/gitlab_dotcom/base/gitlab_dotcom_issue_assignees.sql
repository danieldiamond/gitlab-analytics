{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT DISTINCT
    user_id,
    issue_id
  FROM {{ source('gitlab_dotcom', 'issue_assignees') }}

), renamed AS (

    SELECT
      md5(user_id || issue_id) AS user_issue_relation_id,
      user_id :: integer       AS user_id,
      issue_id :: integer      AS issue_id

    FROM source


)

SELECT DISTINCT *
FROM renamed
