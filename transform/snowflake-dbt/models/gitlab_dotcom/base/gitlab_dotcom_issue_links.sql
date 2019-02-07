WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.issue_links

), renamed AS (

    SELECT
      id :: integer                      as issue_link_id,
      source_id :: integer               as source_id,
      target_id :: integer               as target_id,
      created_at :: timestamp            as issue_link_created_at,
      updated_at :: timestamp            as issue_link_updated_at

    FROM source


)

SELECT *
FROM renamed