{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_builds_runner_session') }}

), renamed AS (

    SELECT 
      build_id::INTEGER AS ci_build_id
      url               AS url
      certificate       AS certificate
      "authorization"   AS authorization

    FROM source

)


SELECT *
FROM renamed
