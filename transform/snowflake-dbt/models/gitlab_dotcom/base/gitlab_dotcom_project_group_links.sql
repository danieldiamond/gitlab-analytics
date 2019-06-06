WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_group_links') }}

), renamed AS (

    SELECT

      id :: integer                                     as project_group_link_id,
      project_id :: integer                             as project_id,
      group_id :: integer                               as group_id,
      group_access :: integer                           as group_access,
      created_at :: timestamp                           as project_features_created_at,
      updated_at :: timestamp                           as project_features_updated_at,
      expires_at :: timestamp                           as expires_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed