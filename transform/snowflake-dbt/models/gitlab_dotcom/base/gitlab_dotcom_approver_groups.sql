WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'approver_groups') }}

), renamed AS (

    SELECT
      id :: integer                   as approver_group_id,
      target_type,
      group_id :: integer             as group_id,
      created_at :: timestamp         as approver_group_created_at,
      updated_at :: timestamp         as approver_group_updated_at

    FROM source
    WHERE rank_in_key = 1

)


SELECT  *
FROM renamed
