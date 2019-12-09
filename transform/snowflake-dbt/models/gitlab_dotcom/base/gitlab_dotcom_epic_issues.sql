{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'epic_issues') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                       AS epic_issues_relation_id,
      epic_id::INTEGER                  AS epic_id,
      issue_id::INTEGER                 AS issue_id,
      relative_position::INTEGER        AS epic_issue_relative_position

    FROM source

)


SELECT *
FROM renamed
