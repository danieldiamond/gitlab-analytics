{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_group_links') }}

), renamed AS (

    SELECT

      id :: integer                                     AS project_group_link_id,
      project_id :: integer                             AS project_id,
      group_id :: integer                               AS group_id,
      group_access :: integer                           AS group_access,
      created_at :: timestamp                           AS project_features_created_at,
      updated_at :: timestamp                           AS project_features_updated_at,
      expires_at :: timestamp                           AS expires_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
