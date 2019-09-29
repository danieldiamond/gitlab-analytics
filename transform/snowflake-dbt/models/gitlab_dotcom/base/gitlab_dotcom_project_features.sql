{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_features') }}

), renamed AS (

    SELECT

      id::INTEGER                                     AS project_feature_id,
      project_id::INTEGER                             AS project_id,
      merge_requests_access_level::INTEGER            AS merge_requests_access_level,
      issues_access_level::INTEGER                    AS issues_access_level,
      wiki_access_level::INTEGER                      AS wiki_access_level,
      snippets_access_level::INTEGER                  AS snippets_access_level,
      builds_access_level::INTEGER                    AS builds_access_level,
      repository_access_level::INTEGER                AS repository_access_level,
      created_at::TIMESTAMP                           AS project_features_created_at,
      updated_at::TIMESTAMP                           AS project_features_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
