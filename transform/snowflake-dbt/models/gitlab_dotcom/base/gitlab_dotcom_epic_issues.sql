{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'epic_issues') }}

), renamed AS (

    SELECT
      id::INTEGER                       AS epic_issues_relation_id,
      epic_id::INTEGER                  AS epic_id,
      issue_id::INTEGER                 AS issue_id,
      relative_position::INTEGER        AS epic_issue_relative_position

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
