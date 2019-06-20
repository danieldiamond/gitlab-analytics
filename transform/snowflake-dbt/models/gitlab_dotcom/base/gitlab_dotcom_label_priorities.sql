WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'label_priorities') }}

), renamed AS (

    SELECT

      id :: integer                           as label_priority_id,
      project_id :: integer                   as project_id,
      label_id :: integer                     as label_id,
      priority :: integer                     as priority,
      created_at :: timestamp                 as label_priority_created_at,
      updated_at :: timestamp                 as label_priority_updated_at


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
