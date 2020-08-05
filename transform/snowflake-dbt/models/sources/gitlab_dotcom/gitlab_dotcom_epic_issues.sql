WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'epic_issues') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                       AS epic_issues_relation_id,
      epic_id::NUMBER                  AS epic_id,
      issue_id::NUMBER                 AS issue_id,
      relative_position::NUMBER        AS epic_issue_relative_position

    FROM source

)


SELECT *
FROM renamed
