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
      id :: integer                       AS epic_issues_relation_id,
      epic_id :: integer                  AS epic_id,
      issue_id :: integer                 AS issue_id,
      relative_position :: integer        AS epic_issue_relative_position

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
