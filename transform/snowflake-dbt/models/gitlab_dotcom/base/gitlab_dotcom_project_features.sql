WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.project_features

), renamed AS (

    SELECT

      id :: integer                                     as project_feature_id,
      project_id :: integer                             as project_id,
      merge_requests_access_level :: integer            as merge_requests_access_level,
      issues_access_level :: integer                    as issues_access_level,
      wiki_access_level :: integer                      as wiki_access_level,
      snippets_access_level :: integer                  as snippets_access_level,
      builds_access_level :: integer                    as builds_access_level,
      repository_access_level :: integer                as repository_access_level,
      created_at :: timestamp                           as project_features_created_at,
      updated_at :: timestamp                           as project_features_updated_at

    FROM source


)

SELECT *
FROM renamed