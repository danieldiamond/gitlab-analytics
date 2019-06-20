WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_auto_devops') }}

), renamed AS (

    SELECT

      project_id :: integer              as project_id,
      created_at :: timestamp            as project_auto_devops_created_at,
      updated_at :: timestamp            as project_auto_devops_updated_at,
      enabled :: boolean                 as has_auto_devops_enabled

    FROM source
    WHERE rank_in_key = 1
)

SELECT *
FROM renamed
