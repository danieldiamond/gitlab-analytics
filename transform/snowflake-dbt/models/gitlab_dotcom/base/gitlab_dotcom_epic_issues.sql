WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.epic_issues

), renamed AS (

    SELECT
      id :: integer                       as epic_issues_relation_id,
      epic_id :: integer                  as epic_id,
      issue_id :: integer                 as issue_id,
      relative_position :: integer        as epic_issue_relative_position

    FROM source


)

SELECT *
FROM renamed