-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)
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

      md5(user_id :: integer || '-' || project_id :: integer || '-' || access_level :: integer) AS user_project_access_relation_id, -- without the extra '-' two rows result in the same hash
      user_id :: integer                                                                        AS user_id,
      project_id :: integer                                                                     AS project_id,
      access_level :: integer                                                                   AS access_level

    FROM source

)

SELECT DISTINCT *
FROM renamed