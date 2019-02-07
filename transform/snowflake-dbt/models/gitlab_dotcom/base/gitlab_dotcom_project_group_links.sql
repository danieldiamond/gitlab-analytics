WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.project_group_links

), renamed AS (

    SELECT

      id :: integer                                     as project_group_link_id,
      project_id :: integer                             as project_id,
      group_id :: integer                               as group_id,
      group_access :: integer                           as group_access,
      created_at :: timestamp                           as project_features_created_at,
      updated_at :: timestamp                           as project_features_updated_at,
      TRY_CAST(expires_at as timestamp)                 as expires_at

    FROM source


)

SELECT *
FROM renamed