WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'project_features') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                                     AS project_feature_id,
      project_id::NUMBER                             AS project_id,
      merge_requests_access_level::NUMBER            AS merge_requests_access_level,
      issues_access_level::NUMBER                    AS issues_access_level,
      wiki_access_level::NUMBER                      AS wiki_access_level,
      snippets_access_level::NUMBER                  AS snippets_access_level,
      builds_access_level::NUMBER                    AS builds_access_level,
      repository_access_level::NUMBER                AS repository_access_level,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at

    FROM source

)

SELECT *
FROM renamed
