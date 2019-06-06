WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'epic_issues') }}

), renamed AS (

    SELECT
      id :: integer                       as epic_issues_relation_id,
      epic_id :: integer                  as epic_id,
      issue_id :: integer                 as issue_id,
      relative_position :: integer        as epic_issue_relative_position

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed