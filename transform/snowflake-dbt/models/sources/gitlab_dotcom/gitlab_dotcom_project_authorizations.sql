WITH source AS (

  SELECT DISTINCT
    user_id,
    project_id,
    access_level
  FROM {{ source('gitlab_dotcom', 'project_authorizations') }}

), renamed AS (

    SELECT

      MD5(user_id::NUMBER || '-' || project_id::NUMBER || '-' || access_level::NUMBER)     AS user_project_access_relation_id, -- without the extra '-' two rows result in the same hash
      user_id::NUMBER                                                                        AS user_id,
      project_id::NUMBER                                                                     AS project_id,
      access_level::NUMBER                                                                   AS access_level

    FROM source

)

SELECT DISTINCT *
FROM renamed
