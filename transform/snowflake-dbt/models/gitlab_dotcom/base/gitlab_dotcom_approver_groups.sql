WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.approver_groups

), renamed AS (

    SELECT
      id :: integer                   as approver_group_id,
      target_type,
      group_id :: integer             as group_id,
      created_at :: timestamp         as approver_group_created_at,
      updated_at :: timestamp         as approver_group_updated_at

    FROM source


)

SELECT *
FROM renamed