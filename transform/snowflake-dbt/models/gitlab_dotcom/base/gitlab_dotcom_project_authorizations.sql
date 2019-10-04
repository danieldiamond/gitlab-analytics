{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT DISTINCT
    user_id,
    project_id,
    access_level
  FROM {{ source('gitlab_dotcom', 'project_authorizations') }}

), renamed AS (

    SELECT

      md5(user_id::INTEGER || '-' || project_id::INTEGER || '-' || access_level::INTEGER)     AS user_project_access_relation_id, -- without the extra '-' two rows result in the same hash
      user_id::INTEGER                                                                        AS user_id,
      project_id::INTEGER                                                                     AS project_id,
      access_level::INTEGER                                                                   AS access_level

    FROM source

)

SELECT DISTINCT *
FROM renamed