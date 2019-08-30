{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_features') }}

), renamed AS (

    SELECT

      id :: integer                                     AS project_feature_id,
      project_id :: integer                             AS project_id,
      merge_requests_access_level :: integer            AS merge_requests_access_level,
      issues_access_level :: integer                    AS issues_access_level,
      wiki_access_level :: integer                      AS wiki_access_level,
      snippets_access_level :: integer                  AS snippets_access_level,
      builds_access_level :: integer                    AS builds_access_level,
      repository_access_level :: integer                AS repository_access_level,
      created_at :: timestamp                           AS project_features_created_at,
      updated_at :: timestamp                           AS project_features_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
